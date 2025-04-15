# analyzer.py
import asyncio
import os
import time
import datetime
import pytz
import json
import requests
import gspread
import pandas as pd
import google.generativeai as genai
import backoff
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import csv # Import csv module
import re # Import regex module for cleaning
import argparse # Add argparse

# --- Load Environment Variables --- #
load_dotenv()

# --- Argument Parser --- #
parser = argparse.ArgumentParser(description='Analyze and rewrite content from different platforms.')
parser.add_argument('--platform', type=str.lower, required=True, choices=['twitter', 'reddit'],
                    help='Specify the platform to process (twitter or reddit).')
args = parser.parse_args()
PLATFORM = args.platform
print(f"Running Analyzer for platform: {PLATFORM}")

# --- Configuration based on Platform --- #
print(f"Loading configuration for {PLATFORM}...")
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE_PATH', 'service_account.json')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_CONCURRENT_REQUESTS = int(os.getenv('GEMINI_CONCURRENT_REQUESTS', '1'))
TARGET_TIMEZONE_STR = os.getenv('TARGET_TIMEZONE', 'UTC')
# Load the single main Google Sheet URL
GOOGLE_SHEETS_URL = os.getenv('GOOGLE_SHEETS_URL')

# Platform-specific settings (Sheet Names, Local File)
if PLATFORM == 'twitter':
    SOURCE_SHEET_NAMES_STR = os.getenv('TWITTER_SOURCE_SHEET_NAMES', 'Sheet1')
    TARGET_SHEET_NAME = os.getenv('TWITTER_ANALYZED_SHEET_NAME', 'Analyzed_Twitter')
    LOCAL_STATE_FILE = os.getenv('TWITTER_LOCAL_STATE_FILE', 'twitter_processed_state.csv')
elif PLATFORM == 'reddit':
    SOURCE_SHEET_NAMES_STR = os.getenv('REDDIT_SOURCE_SHEET_NAMES', 'Sheet_Reddit_Raw')
    TARGET_SHEET_NAME = os.getenv('REDDIT_ANALYZED_SHEET_NAME', 'Analyzed_Reddit')
    LOCAL_STATE_FILE = os.getenv('REDDIT_LOCAL_STATE_FILE', 'reddit_processed_state.csv')
else:
    # Should not happen due to argparse choices, but as a safeguard
    print(f"Error: Invalid platform specified: {PLATFORM}")
    exit()

print(f"  Using Google Sheet URL: {GOOGLE_SHEETS_URL}")
print(f"  Source Sheet Names: {SOURCE_SHEET_NAMES_STR}")
print(f"  Target Analyzed Sheet Name: {TARGET_SHEET_NAME}")
print(f"  Local State File: {LOCAL_STATE_FILE}")

# --- Constants --- #
# Define columns for the target analyzed sheet - Keep this standard
# (ensure this list matches column names used in processing logic)
TARGET_COLUMNS = [
    "Processed Timestamp", "Original Username", "Original Display Name",
    "First Tweet Timestamp", "Combined Original Text", "First Tweet URL",
    "Likes (First Tweet)", "Retweets (First Tweet)", "Replies (First Tweet)",
    "Quotes (First Tweet)", "Bookmarks (First Tweet)", "Views (First Tweet)",
    "Content Type", # Thread, Original Tweet, Reply, Quote Tweet, Retweet, Reddit Post, etc.
    "Conversation ID",
    "Rewritten EN", "Rewritten RU",
    "Source Row Count", # How many source rows were combined (1 for singles, >1 for threads)
    "Platform", # Added Platform to target columns
    "Subreddit", "Score", "Num Comments", "Post ID" # Added Reddit-specific columns
]

# --- Validate essential config --- #
# Simplified validation for single URL
if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEETS_URL]):
    print("Error: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and GOOGLE_SHEETS_URL must be set.")
    exit()
if not SERVICE_ACCOUNT_FILE:
     print("Error: SERVICE_ACCOUNT_FILE_PATH must be set.")
     exit()
if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY must be set.")
    exit()
if GEMINI_CONCURRENT_REQUESTS <= 0:
    print("Warning: GEMINI_CONCURRENT_REQUESTS must be positive. Setting to 1.")
    GEMINI_CONCURRENT_REQUESTS = 1

# --- Initialize Services --- #
gemini_model = None
worksheet_tgt = None
TARGET_TIMEZONE = pytz.utc

# Telegram
def send_telegram_notification(message):
    """Sends a notification message to the configured Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram token or chat ID not configured. Skipping notification.")
        return
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4096
    truncated_message = message[:max_len] if len(message) > max_len else message
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': truncated_message}
    try:
        response = requests.post(send_url, json=payload, timeout=15)
        response.raise_for_status()
        print(f"Sent Telegram notification: {truncated_message[:70].splitlines()[0]}...")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Telegram notification: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during Telegram notification sending: {e}")

# Google Sheets Initialization
# Authenticate using service account
print("Authenticating with Google...")
sh = None # Single Spreadsheet object
try:
    SHEET_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file'
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)

    # Open the single Google Sheet file
    print(f"Opening Google Sheet file: {GOOGLE_SHEETS_URL}")
    sh = gc.open_by_url(GOOGLE_SHEETS_URL)

    # Get/create the platform-specific TARGET ANALYZED worksheet within this file
    try:
        worksheet_tgt = sh.worksheet(TARGET_SHEET_NAME)
        print(f"Target sheet '{TARGET_SHEET_NAME}' found in the sheet file.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Target sheet '{TARGET_SHEET_NAME}' not found. Creating it...")
        worksheet_tgt = sh.add_worksheet(title=TARGET_SHEET_NAME, rows="1", cols=str(len(TARGET_COLUMNS)))
        worksheet_tgt.append_row(TARGET_COLUMNS, value_input_option='USER_ENTERED')
        print(f"Target sheet '{TARGET_SHEET_NAME}' created with headers.")

except Exception as e:
    error_msg = f"CRITICAL ERROR: Failed to authenticate or open/prepare Google Sheet: {e}"
    print(error_msg)
    send_telegram_notification(f"🚨 {error_msg}")
    exit()

# Gemini
try:
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel('gemini-1.5-flash') # Or your preferred model
    print("Gemini API configured successfully.")
except Exception as e:
    error_msg = f"CRITICAL ERROR: Failed to configure Gemini API: {e}"
    print(error_msg)
    send_telegram_notification(f"🚨 {error_msg}")
    exit()

# Timezone
try:
    TARGET_TIMEZONE = pytz.timezone(TARGET_TIMEZONE_STR)
except pytz.exceptions.UnknownTimeZoneError:
    print(f"Warning: Unknown timezone '{TARGET_TIMEZONE_STR}'. Defaulting to UTC.")
    # TARGET_TIMEZONE remains UTC

# --- Helper Functions --- #
# Function to load processed URLs from various sources
def load_processed_urls(df):
    """Extracts a set of successfully processed URLs from a DataFrame."""
    processed_urls = set()
    # Ensure target columns exist before attempting to filter
    required_cols = ['First Tweet URL', 'Rewritten EN', 'Rewritten RU']
    if df is not None and not df.empty and all(col in df.columns for col in required_cols):
        try:
            # Convert rewrite columns to string before checking startswith
            df['Rewritten EN'] = df['Rewritten EN'].astype(str)
            df['Rewritten RU'] = df['Rewritten RU'].astype(str)

            processed_df = df[
                df['First Tweet URL'].notna() &
                (df['Rewritten EN'].fillna('') != '') &
                (df['Rewritten RU'].fillna('') != '') &
                (~df['Rewritten EN'].str.startswith('Error:', na=False)) &
                (~df['Rewritten RU'].str.startswith('Error:', na=False))
            ]
            processed_urls = set(processed_df['First Tweet URL'].astype(str))
        except Exception as e:
            print(f"Error processing DataFrame in load_processed_urls: {e}")
            # Fallback to empty set if processing fails
            processed_urls = set()
    return processed_urls

# --- Gemini Rewriting Function --- #
@backoff.on_exception(backoff.expo, Exception, max_tries=3, jitter=backoff.full_jitter, on_giveup=lambda details: print(f"Gemini API call failed after {details['tries']} tries. Error: {details['exception']}"))
async def rewrite_text_gemini(original_text, content_type, semaphore):
    """Rewrites the given text into English and Russian using the Gemini API, respecting the semaphore and Rahmet Labs voice."""
    # Acquire semaphore before proceeding
    async with semaphore:
        print(f"  Semaphore acquired. Rewriting {content_type} (length: {len(original_text)} chars) using Rahmet Labs voice...")
        rewritten_en = "Error: Rewrite Failed (EN)"
        rewritten_ru = "Error: Rewrite Failed (RU)"

        # Refined prompts based on rahmetlabs_character.json
        prompt_en = f"""Act as the Rahmet Labs AI Copywriter. Your voice is direct, practical, tech-savvy but grounded (no hype), professionally informal, transparent, and confidently focused on tangible outcomes. Analyze the following raw input (notes, tweet, news) and rewrite it into clear, concise, engaging copy reflecting the Rahmet Labs brand. Focus on problem/solution framing and real-world applications. A touch of dry wit or skepticism towards hype is okay. **Format the output for readability using appropriate paragraph breaks.** Ensure the output does not contain any hashtags (#). AVOID: Corporate buzzwords, marketing hype (revolutionary, game-changer, etc.), vague platitudes, overly complex sentences, passive voice. Transform the input into polished, authentic Rahmet Labs communication (e.g., for social media or blog posts):\n\n---\n{original_text}\n---"""
        prompt_ru = f"""Действуй как AI Копирайтер Rahmet Labs. Твой стиль – прямой, практичный, технически грамотный, но приземленный (без хайпа), профессионально-неформальный, прозрачный и уверенно сфокусированный на ощутимых результатах. Проанализируй следующий необработанный текст (заметки, твит, новость) и перепиши его в ясный, краткий, привлекательный текст, отражающий бренд Rahmet Labs. Сконцентрируйся на формате проблема/решение и реальных применениях. Допускается легкая сухая ирония или скептицизм по отношению к хайпу. **Отформатируй вывод для читабельности, используя соответствующие разрывы абзацев.** Убедись, что в выводе нет хештегов (#). ИЗБЕГАЙ: Корпоративного жаргона, маркетингового хайпа (революционный, меняющий правила игры и т.д.), расплывчатых фраз, излишне сложных предложений, пассивного залога. Преобразуй исходный текст в отполированное, аутентичное сообщение от Rahmet Labs (например, для соцсетей или блога):\n\n---\n{original_text}\n---"""

        try:
            # Run synchronous SDK calls in a separate thread
            response_en = await asyncio.to_thread(gemini_model.generate_content, prompt_en, request_options={'timeout': 180})
            rewritten_en = response_en.text.strip() # Added strip()
            print("    English rewrite generated.")

            # Small delay *inside* semaphore lock if needed to further space calls
            await asyncio.sleep(1) # Consider if this delay is necessary with semaphore

            response_ru = await asyncio.to_thread(gemini_model.generate_content, prompt_ru, request_options={'timeout': 180})
            rewritten_ru = response_ru.text.strip() # Added strip() here too
            print("    Russian rewrite generated.")

        except Exception as e:
            print(f"    ERROR calling Gemini API: {e}")
            raise # Re-raise for backoff

        print(f"  Semaphore released for {content_type} (length: {len(original_text)} chars).")
        return rewritten_en, rewritten_ru
    # Semaphore is automatically released when exiting the 'async with' block

# --- Main Processing Logic --- #
async def process_data():
    """Reads source data, processes threads, rewrites text saving locally, and syncs to target sheet."""
    print("\n--- Starting Data Processing ---")
    start_time = time.time()

    # 1. Read Source Data from Specific Sheets in the single RAW file
    source_sheet_names = [name.strip() for name in SOURCE_SHEET_NAMES_STR.split(',') if name.strip()]
    print(f"Reading data for {PLATFORM} from source sheets: {source_sheet_names}...")
    all_source_dfs = []
    total_source_rows = 0

    for sheet_name in source_sheet_names:
        print(f"  Attempting to read sheet: '{sheet_name}'...")
        try:
            # Use the single 'sh' object opened earlier
            worksheet_src = sh.worksheet(sheet_name)
            data_src = worksheet_src.get_all_values()
            if len(data_src) < 2:
                print(f"  Sheet '{sheet_name}' is empty or only contains headers. Skipping.")
                continue

            header = data_src[0]
            df_sheet = pd.DataFrame(data_src[1:], columns=header)

            # Standardization: Add missing columns from TARGET_COLUMNS, keep existing ones
            current_cols = df_sheet.columns.tolist()
            added_cols = False
            for col in TARGET_COLUMNS:
                if col not in current_cols:
                    df_sheet[col] = '' # Add missing target columns
                    added_cols = True
            
            # No need to explicitly select columns here, keep all read + added target ones

            all_source_dfs.append(df_sheet)
            print(f"  Read {len(df_sheet)} rows from sheet '{sheet_name}'. Added missing target columns.")
            total_source_rows += len(df_sheet)

        except gspread.exceptions.WorksheetNotFound:
            print(f"  Warning: Source sheet '{sheet_name}' not found. Skipping.")
        except Exception as e:
            error_msg = f"  ERROR reading data from source sheet '{sheet_name}': {e}"
            print(error_msg)
            send_telegram_notification(f"🚨 {error_msg}")
            # Decide if we should continue or exit? For now, continue with other sheets.

    if not all_source_dfs:
        print("No data read from any source sheets. Exiting.")
        send_telegram_notification("ℹ️ Analyzer run finished: No source data found.")
        return

    # Concatenate source dataframes
    df_src = pd.concat(all_source_dfs, ignore_index=True)
    print(f"Combined data for {PLATFORM}. Total rows: {len(df_src)}")

    # 1b. Load Platform-Specific Local State
    print(f"Attempting to load local state from '{LOCAL_STATE_FILE}'...")
    df_local_state = None
    processed_urls_local = set()
    try:
        if os.path.exists(LOCAL_STATE_FILE) and os.path.getsize(LOCAL_STATE_FILE) > 0:
             dtypes = {col: str for col in TARGET_COLUMNS}
             df_local_state = pd.read_csv(LOCAL_STATE_FILE, dtype=dtypes, keep_default_na=False, na_values=[''])
             df_local_state = df_local_state.fillna('')
             # Ensure all TARGET_COLUMNS exist after load
             for col in TARGET_COLUMNS:
                  if col not in df_local_state.columns:
                       df_local_state[col] = ''
             df_local_state = df_local_state[TARGET_COLUMNS] # Reorder/select
             processed_urls_local = load_processed_urls(df_local_state)
             print(f"Loaded {len(df_local_state)} rows from local state. Found {len(processed_urls_local)} successfully processed URLs.")
        else:
            print("Local state file not found or empty.")
            df_local_state = pd.DataFrame(columns=TARGET_COLUMNS)
            processed_urls_local = set()
    except pd.errors.EmptyDataError:
         print("Local state file is empty.")
         df_local_state = pd.DataFrame(columns=TARGET_COLUMNS)
         processed_urls_local = set()
    except Exception as e:
        print(f"Warning: Could not read or parse local state file '{LOCAL_STATE_FILE}': {e}. Proceeding without local state.")
        # Ensure df_local_state is an empty DataFrame for consistency
        df_local_state = pd.DataFrame(columns=TARGET_COLUMNS)
        processed_urls_local = set()

    # 1c. Read Platform-Specific Target Google Sheet State
    print(f"Reading existing data from target sheet '{TARGET_SHEET_NAME}'...")
    df_gsheet_state = None
    processed_urls_gsheet = set()
    try:
        data_tgt = worksheet_tgt.get_all_values()
        if len(data_tgt) >= 2:
            header_tgt = data_tgt[0]
            df_gsheet_state = pd.DataFrame(data_tgt[1:], columns=header_tgt).astype(str)
            df_gsheet_state = df_gsheet_state.fillna('')
            # Ensure all TARGET_COLUMNS exist
            for col in TARGET_COLUMNS:
                 if col not in df_gsheet_state.columns:
                      df_gsheet_state[col] = ''
            df_gsheet_state = df_gsheet_state[TARGET_COLUMNS]
            processed_urls_gsheet = load_processed_urls(df_gsheet_state)
            print(f"Found {len(processed_urls_gsheet)} successfully processed URLs in target sheet.")
        else:
            print("Target sheet is empty or has no data rows.")
            df_gsheet_state = pd.DataFrame(columns=TARGET_COLUMNS)
            processed_urls_gsheet = set()
    except Exception as e:
        print(f"Warning: Could not read or parse target sheet '{TARGET_SHEET_NAME}': {e}.")

    # 1d. Combine Processed URLs
    processed_urls = processed_urls_local.union(processed_urls_gsheet)
    print(f"Total unique processed URLs from local and GSheet: {len(processed_urls)}")

    # 2. Pre-process Combined Source Data
    print("Pre-processing combined source data...")
    try:
        # Ensure required columns for processing exist (only check for consolidation inputs now)
        required_process_cols = ['Original Username', 'Conversation ID', 'Tweet Text'] # Removed timestamp for now
        if not all(col in df_src.columns for col in required_process_cols):
            missing_cols = [col for col in required_process_cols if col not in df_src.columns]
            print(f"ERROR: Source DataFrame missing required columns for processing: {missing_cols}")
            return

        # Convert numeric columns (using TARGET_COLUMNS definition)
        num_cols_in_target = ['Likes (First Tweet)', 'Retweets (First Tweet)', 'Replies (First Tweet)', 'Quotes (First Tweet)', 'Bookmarks (First Tweet)', 'Views (First Tweet)', 'Score', 'Num Comments']
        for col in num_cols_in_target:
            if col in df_src.columns:
                 # Use pd.to_numeric, handle errors by coercing to NaN, then fill NaN with 0
                 df_src[col] = pd.to_numeric(df_src[col], errors='coerce').fillna(0).astype(int)
            else:
                 df_src[col] = 0

        # Ensure Conversation ID is string
        df_src['Conversation ID'] = df_src['Conversation ID'].astype(str)
        # Ensure Tweet Text is string
        df_src['Tweet Text'] = df_src['Tweet Text'].astype(str)

    except Exception as e:
        error_msg = f"ERROR during data pre-processing: {e}"
        print(error_msg)
        import traceback
        tb_str = traceback.format_exc()
        send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:1000]}\n```")
        return

    # 3. Identify and Consolidate Threads
    print("Identifying and consolidating threads (filtering replies to others)...")
    processed_rows = []
    # Ensure columns needed for grouping exist before grouping
    grouping_cols = ['Original Username', 'Conversation ID']
    if not all(col in df_src.columns for col in grouping_cols):
        missing_group_cols = [col for col in grouping_cols if col not in df_src.columns]
        print(f"ERROR: Cannot group data, missing columns: {missing_group_cols}")
        return
    grouped = df_src.groupby(grouping_cols)

    for name, group in grouped:
        username, conv_id = name
        group = group.sort_values(by='First Tweet Timestamp') # Ensure sorted within group
        author_username = username # Get the author username for this group

        # Filter out tweets that are clearly replies starting with @other_user
        # Regex: ^\s* : start of string, optional whitespace
        # @ : literal @
        # (?!(?:{re.escape(author_username)}\b)) : negative lookahead - username is NOT the author's own username (case-insensitive later)
        # (?:...) : non-capturing group
        # \b : word boundary to avoid partial matches
        # [\w]{1,15} : matches a valid Twitter username (1-15 alphanumeric chars + _)
        # Match case-insensitively
        reply_to_other_pattern = rf'^\s*@(?!(?:{re.escape(author_username)}\b))([\w]{{1,15}})'
        core_thread_tweets = group[~group['Tweet Text'].str.contains(reply_to_other_pattern, case=False, na=False, regex=True)].copy()

        if core_thread_tweets.empty:
            # This conversation by the user consisted ONLY of replies to others, skip.
            # print(f"  Skipping group {name} - only replies to others.") # Optional verbose log
            continue

        # Determine if it's a thread based on remaining tweets
        is_thread = len(core_thread_tweets) > 1

        if is_thread:
            first_tweet = core_thread_tweets.iloc[0]
            # Combine text ONLY from the core thread tweets
            combined_text = "\n\n---\n\n".join(core_thread_tweets['Tweet Text'].astype(str))
            processed_row = {
                "Processed Timestamp": datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z'),
                "Original Username": first_tweet['Original Username'],
                "Original Display Name": first_tweet['Display Name'],
                "First Tweet Timestamp": first_tweet['First Tweet Timestamp'],
                "Combined Original Text": combined_text,
                "First Tweet URL": first_tweet['First Tweet URL'],
                "Likes (First Tweet)": first_tweet['Likes (First Tweet)'],
                "Retweets (First Tweet)": first_tweet['Retweets (First Tweet)'],
                "Replies (First Tweet)": first_tweet['Replies (First Tweet)'],
                "Quotes (First Tweet)": first_tweet['Quotes (First Tweet)'],
                "Bookmarks (First Tweet)": first_tweet['Bookmarks (First Tweet)'],
                "Views (First Tweet)": first_tweet['Views (First Tweet)'],
                "Content Type": "Thread", # Mark as Thread
                "Conversation ID": conv_id,
                "Rewritten EN": "",
                "Rewritten RU": "",
                "Source Row Count": len(core_thread_tweets), # Count only core tweets
                "Platform": PLATFORM,
                "Subreddit": "",
                "Score": "",
                "Num Comments": "",
                "Post ID": ""
            }
            processed_rows.append(processed_row)
        else:
            # Only one tweet left after filtering, treat as single
            single_tweet = core_thread_tweets.iloc[0]
            processed_row = {
                "Processed Timestamp": datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z'),
                "Original Username": single_tweet['Original Username'],
                "Original Display Name": single_tweet['Display Name'],
                "First Tweet Timestamp": single_tweet['First Tweet Timestamp'],
                "Combined Original Text": single_tweet['Tweet Text'],
                "First Tweet URL": single_tweet['First Tweet URL'],
                "Likes (First Tweet)": single_tweet['Likes (First Tweet)'],
                "Retweets (First Tweet)": single_tweet['Retweets (First Tweet)'],
                "Replies (First Tweet)": single_tweet['Replies (First Tweet)'],
                "Quotes (First Tweet)": single_tweet['Quotes (First Tweet)'],
                "Bookmarks (First Tweet)": single_tweet['Bookmarks (First Tweet)'],
                "Views (First Tweet)": single_tweet['Views (First Tweet)'],
                "Content Type": single_tweet['Tweet Type'], # Use original type
                "Conversation ID": conv_id,
                "Rewritten EN": "",
                "Rewritten RU": "",
                "Source Row Count": 1,
                "Platform": PLATFORM,
                "Subreddit": "",
                "Score": "",
                "Num Comments": "",
                "Post ID": ""
            }
            processed_rows.append(processed_row)

    if not processed_rows:
        print("No processable data found after grouping source data.")
        # Run final sync in case local state needs uploading
        await sync_local_to_gsheet(worksheet_tgt)
        return

    df_consolidated = pd.DataFrame(processed_rows)
    # Ensure df_consolidated has all TARGET_COLUMNS before proceeding
    for col in TARGET_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = '' # Add missing columns
    df_consolidated = df_consolidated[TARGET_COLUMNS]
    print(f"Consolidated source data into {len(df_consolidated)} rows (threads/singles).")

    # 3a. Parse Timestamps and Sort Consolidated Data
    print("Parsing timestamps and sorting consolidated data...")
    try:
        # Define parsing function here (or keep globally if preferred)
        def parse_datetime(ts_str):
            try:
                return pd.to_datetime(ts_str)
            except Exception:
                 try:
                     return pd.to_datetime(ts_str.split(' ')[0] + ' ' + ts_str.split(' ')[1])
                 except Exception:
                     return pd.NaT

        # Apply parsing to the correct column
        df_consolidated['First Tweet Timestamp DT'] = df_consolidated['First Tweet Timestamp'].apply(parse_datetime)

        # Drop rows where timestamp parsing failed
        rows_before_sort = len(df_consolidated)
        df_consolidated.dropna(subset=['First Tweet Timestamp DT'], inplace=True)
        if len(df_consolidated) < rows_before_sort:
            print(f"Warning: Dropped {rows_before_sort - len(df_consolidated)} rows due to unparseable timestamps after consolidation.")

        # Sort the consolidated data
        df_consolidated.sort_values(by=['Original Username', 'Conversation ID', 'First Tweet Timestamp DT'], inplace=True)
        print("Consolidated data sorted.")

    except Exception as e:
        error_msg = f"ERROR during timestamp parsing/sorting of consolidated data: {e}"
        print(error_msg)
        # ... [error handling] ...
        return

    # 3b. Filter out already processed items
    # Uses 'First Tweet URL' which should be present from standardization/consolidation
    df_to_process = df_consolidated[~df_consolidated['First Tweet URL'].isin(processed_urls)].copy()
    print(f"Filtered down to {len(df_to_process)} new items to process.")

    # 3c. Filter by Content Type before rewriting
    print(f"Filtering {len(df_to_process)} items based on platform-specific content types...")
    if PLATFORM == 'twitter':
        content_types_to_rewrite = ['Original Tweet', 'Thread']
    elif PLATFORM == 'reddit':
        # Assuming the reddit scraper sets Tweet Type to 'Reddit Post'
        content_types_to_rewrite = ['Reddit Post'] 
    else:
        content_types_to_rewrite = [] # Should not happen

    df_typed_filtered = df_to_process[df_to_process['Content Type'].isin(content_types_to_rewrite)].copy()
    print(f"Filtered down to {len(df_typed_filtered)} items matching desired content types: {content_types_to_rewrite}")

    if df_typed_filtered.empty:
        print("No new items match the desired content types (Original Tweet, Thread).")
        await sync_local_to_gsheet(worksheet_tgt) # Still sync in case local needs upload
        send_telegram_notification("ℹ️ Analyzer run finished: No new Original Tweets or Threads found.")
        return

    # 3d. Apply Additional Content Filters (Length, Relevance Keywords)
    print(f"Applying additional content filters to {len(df_typed_filtered)} items...")

    MIN_CONTENT_LENGTH = 50 # Minimum characters excluding URLs/separators
    RELEVANT_KEYWORDS = [
        'ai', 'agi', 'openai', 'google', 'gemini', 'claude', 'mistral', 'llm',
        'model', 'automation', ' n8n', 'python', 'api', 'workflow', 'data',
        'tech', 'business', 'startup', 'rahmetlabs', 'scraping', 'analyze',
        'process', 'update', 'news', 'release', 'research', 'paper', 'opinion',
        'thought', 'develop', 'build', 'future', 'risk', 'safety', 'alignment',
        'code', 'coding', 'launch', 'feature', 'limit', 'rate limit', 'context window',
        'token', 'prompt', 'engineer', 'benchmark', 'test'
    ] # Expanded keyword list

    # Helper to clean text for length/relevance check
    def clean_text_for_filtering(text):
        text = str(text)
        # Remove URLs
        text = re.sub(r'http\S+|www.\S+', '', text)
        # Remove potential --- separators
        text = text.replace('---', '')
        # Remove extra whitespace
        text = ' '.join(text.split())
        return text.strip()

    # Apply cleaning
    df_typed_filtered['Cleaned Text'] = df_typed_filtered['Combined Original Text'].apply(clean_text_for_filtering)

    # Apply Length Filter
    df_length_filtered = df_typed_filtered[df_typed_filtered['Cleaned Text'].str.len() >= MIN_CONTENT_LENGTH].copy()
    removed_by_length = len(df_typed_filtered) - len(df_length_filtered)
    if removed_by_length > 0:
        print(f"  {removed_by_length} items removed by length filter (<{MIN_CONTENT_LENGTH} chars).")

    # Apply Keyword Filter
    keyword_pattern = '|'.join([re.escape(k) for k in RELEVANT_KEYWORDS]) # Escape keywords for regex
    df_final_filtered = df_length_filtered[df_length_filtered['Combined Original Text'].str.contains(keyword_pattern, case=False, na=False)].copy()
    removed_by_keyword = len(df_length_filtered) - len(df_final_filtered)
    if removed_by_keyword > 0:
        print(f"  {removed_by_keyword} items removed by keyword filter.")

    # Apply Prompt/Structure Filter (New)
    prompt_markers = ['# Prompt', '<Role>', '<Instructions>', '<Context>']
    prompt_pattern = '|'.join([re.escape(m) for m in prompt_markers])
    # Also check for more than one code block as an indicator
    df_pre_prompt_filter = df_final_filtered.copy() # Keep track before this filter
    df_final_filtered = df_final_filtered[
        ~df_final_filtered['Combined Original Text'].str.contains(prompt_pattern, case=False, na=False) &
        (df_final_filtered['Combined Original Text'].str.count('```') <= 2) # Allow zero or one code block
    ].copy()
    removed_by_prompt_filter = len(df_pre_prompt_filter) - len(df_final_filtered)
    if removed_by_prompt_filter > 0:
        print(f"  {removed_by_prompt_filter} items removed by prompt structure filter.")

    # Clean up temporary column
    # df_final_filtered = df_final_filtered.drop(columns=['Cleaned Text']) # Removed earlier, ensure no error if run again
    if 'Cleaned Text' in df_final_filtered.columns:
         df_final_filtered = df_final_filtered.drop(columns=['Cleaned Text'])

    print(f"Filtered down to {len(df_final_filtered)} items meeting all criteria.")

    if df_final_filtered.empty:
        print("No new items match all filtering criteria. Nothing to rewrite.")
        await sync_local_to_gsheet(worksheet_tgt)
        send_telegram_notification("ℹ️ Analyzer run finished: No new relevant Original Tweets or Threads to process.")
        return

    # 4. Rewrite Text & Save Incrementally to Platform-Specific Local CSV
    print(f"Rewriting text for {len(df_final_filtered)} filtered items sequentially and saving to '{LOCAL_STATE_FILE}'...")
    semaphore = asyncio.Semaphore(GEMINI_CONCURRENT_REQUESTS)
    POST_REWRITE_DELAY_SECONDS = 10.0
    processed_count = 0
    failed_count = 0
    total_rows_to_rewrite = len(df_final_filtered)
    headers_written = os.path.exists(LOCAL_STATE_FILE) and os.path.getsize(LOCAL_STATE_FILE) > 0

    # Open local state file in append mode
    with open(LOCAL_STATE_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=TARGET_COLUMNS)
        # Write header only if the file is new/empty
        # Check if headers need to be written (only once per complete run where items are processed)
        needs_header = not headers_written

        for index, row in df_final_filtered.iterrows(): # Iterate over final filtered DataFrame
            # Write header on the very first iteration if needed
            if needs_header:
                 writer.writeheader()
                 headers_written = True
                 needs_header = False # Prevent writing header again

            current_item_num = processed_count + failed_count + 1
            print(f"\nProcessing item {current_item_num}/{total_rows_to_rewrite} (Source Index: {index})...")
            original_text = row['Combined Original Text']
            content_type = row['Content Type']
            rewritten_en = "Error: Skipped"
            rewritten_ru = "Error: Skipped"

            # Prepare base row data from the consolidated row
            result_row = row.to_dict()

            if pd.isna(original_text) or not str(original_text).strip():
                print(f"  Skipping item {current_item_num} due to empty original text.")
                result_row['Rewritten EN'] = "Error: Empty Source Text"
                result_row['Rewritten RU'] = "Error: Empty Source Text"
                failed_count += 1
            else:
                try:
                    en_text, ru_text = await rewrite_text_gemini(str(original_text), content_type, semaphore)
                    result_row['Rewritten EN'] = en_text
                    result_row['Rewritten RU'] = ru_text
                    processed_count += 1
                except Exception as e:
                    print(f"  Rewrite failed for item {current_item_num}: {e}")
                    result_row['Rewritten EN'] = "Error: Rewrite Failed (EN)"
                    result_row['Rewritten RU'] = "Error: Rewrite Failed (RU)"
                    failed_count += 1

            # Ensure all target columns are present before writing
            final_row_for_csv = {col: result_row.get(col, '') for col in TARGET_COLUMNS}

            # Write the processed row immediately to local CSV
            try:
                writer.writerow(final_row_for_csv)
                print(f"  Saved item {current_item_num} to '{LOCAL_STATE_FILE}'.")
            except Exception as write_e:
                 print(f"  ERROR writing item {current_item_num} to local CSV: {write_e}")
                 # Optional: Add more robust error handling here if needed

            # Delay before next item
            if current_item_num < total_rows_to_rewrite:
                print(f"  Waiting {POST_REWRITE_DELAY_SECONDS}s before next item...")
                await asyncio.sleep(POST_REWRITE_DELAY_SECONDS)

    print(f"\nFinished processing loop: {processed_count} successful, {failed_count} failed and saved locally.")

    # 5. Final Sync: Upload missing rows from Local State to Platform-Specific Target Google Sheet
    await sync_local_to_gsheet(worksheet_tgt) # Pass worksheet object

    end_time = time.time()
    duration = end_time - start_time
    success_msg = f"✅ Analyzer finished successfully in {duration:.2f}s. Processed {processed_count} new items. Check '{TARGET_SHEET_NAME}' for synced data."
    print(success_msg)
    send_telegram_notification(success_msg)

async def sync_local_to_gsheet(worksheet_tgt):
    """Reads local state and uploads rows missing from the target Google Sheet."""
    print(f"\n--- Starting Final Sync to Google Sheet '{worksheet_tgt.title}' ---")

    # Read definitive local state
    print(f"Reading local state from '{LOCAL_STATE_FILE}' for final sync...")
    df_local_final = None
    try:
        if os.path.exists(LOCAL_STATE_FILE) and os.path.getsize(LOCAL_STATE_FILE) > 0:
             dtypes = {col: str for col in TARGET_COLUMNS}
             df_local_final = pd.read_csv(LOCAL_STATE_FILE, dtype=dtypes, keep_default_na=False, na_values=[''])
             df_local_final = df_local_final.fillna('')
             print(f"Read {len(df_local_final)} rows from final local state.")
        else:
            print("Local state file not found or empty. Nothing to sync.")
            return
    except Exception as e:
        print(f"ERROR reading final local state file '{LOCAL_STATE_FILE}': {e}. Sync aborted.")
        send_telegram_notification(f"🚨 ERROR reading local state file for final sync: {e}")
        return

    # Read current Google Sheet state
    print("Reading current Google Sheet state for comparison...")
    df_gsheet_current = None
    processed_urls_gsheet_current = set()
    try:
        data_tgt = worksheet_tgt.get_all_values()
        if len(data_tgt) >= 2:
            df_gsheet_current = pd.DataFrame(data_tgt[1:], columns=data_tgt[0]).astype(str)
            df_gsheet_current = df_gsheet_current.fillna('')
            # Use the same helper, but only need URLs this time
            if 'First Tweet URL' in df_gsheet_current.columns:
                 processed_urls_gsheet_current = set(df_gsheet_current['First Tweet URL'].dropna())
            print(f"Found {len(processed_urls_gsheet_current)} URLs currently in Google Sheet.")
        else:
            print("Target Google Sheet is empty or has no data rows.")
    except Exception as e:
        print(f"Warning: Could not read target Google Sheet for final sync: {e}. Assuming empty.")
        processed_urls_gsheet_current = set() # Assume empty if read fails


    # Identify rows in local state missing from Google Sheet
    if df_local_final is None or df_local_final.empty:
         print("Local state is empty. Nothing to upload.")
         return

    # Ensure the URL column exists before filtering
    if 'First Tweet URL' not in df_local_final.columns:
        print("ERROR: 'First Tweet URL' column missing in local state file. Cannot determine rows to upload.")
        return

    df_to_upload = df_local_final[~df_local_final['First Tweet URL'].isin(processed_urls_gsheet_current)].copy()

    if df_to_upload.empty:
        print("No new rows found in local state to upload to Google Sheet. Sheet is up-to-date.")
        return

    print(f"Found {len(df_to_upload)} rows in local state missing from Google Sheet. Preparing upload...")

    # Upload missing rows
    try:
        # Ensure columns are in the correct order and fill NA
        df_final_upload = df_to_upload[TARGET_COLUMNS].fillna('')
        rows_to_append = df_final_upload.values.tolist()

        print(f"Appending {len(rows_to_append)} rows to Google Sheet...")
        worksheet_tgt.append_rows(rows_to_append, value_input_option='USER_ENTERED', table_range='A1') # Append after last row
        print("Successfully appended missing rows to Google Sheet.")

    except Exception as e:
        error_msg = f"ERROR appending rows to target sheet '{worksheet_tgt.title}' during final sync: {e}"
        print(error_msg)
        import traceback
        tb_str = traceback.format_exc()
        send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:1000]}\n```")

# --- Run Main Logic --- #
if __name__ == "__main__":
    send_telegram_notification(f"🚀 Analyzer process starting for {PLATFORM}...")
    try:
        asyncio.run(process_data())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
        send_telegram_notification("🛑 Analyzer stopped by user.")
    except Exception as main_e:
        error_msg = f"CRITICAL UNHANDLED ERROR in analyzer main execution: {main_e}"
        print(error_msg)
        import traceback
        tb_str = traceback.format_exc()
        send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:3500]}\n``` Analyzer stopped.")
