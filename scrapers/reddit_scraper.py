# reddit_scraper.py
import os
import datetime
import pytz
import praw
import gspread
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import time
import requests # For Telegram

print("Reddit Scraper starting...")

# --- Load Environment Variables --- #
load_dotenv()
print("Loaded environment variables.")

# --- Configuration --- #
# Reddit API Credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT') # IMPORTANT: Set a unique user agent in .env

# Scraping Settings
SUBREDDITS_STR = os.getenv('REDDIT_SUBREDDITS', 'MachineLearning,programming,technology,startups,artificial')
POST_LIMIT = int(os.getenv('REDDIT_POST_LIMIT', '25')) # Limit per subreddit
TIME_FILTER = os.getenv('REDDIT_TIMEFILTER', 'day') # e.g., 'hour', 'day', 'week', 'month', 'year', 'all'
TARGET_SUBREDDITS = [sub.strip() for sub in SUBREDDITS_STR.split(',') if sub.strip()]

# Google Sheets Config
GOOGLE_SHEETS_URL = os.getenv('GOOGLE_SHEETS_URL')
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE_PATH', 'service_account.json')
TARGET_SHEET_NAME = os.getenv('REDDIT_TARGET_SHEET_NAME', 'Sheet_Reddit_Raw') # Sheet for raw Reddit data

# General Config
TARGET_TIMEZONE_STR = os.getenv('TARGET_TIMEZONE', 'UTC')

# Telegram Config (Optional, for notifications)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

print(f"Target Subreddits: {TARGET_SUBREDDITS}")
print(f"Post Limit per Subreddit: {POST_LIMIT}")
print(f"Time Filter: {TIME_FILTER}")
print(f"Target Sheet Name: {TARGET_SHEET_NAME}")

# --- Constants --- #
# Define the standard output schema for the Analyzer
OUTPUT_COLUMNS = [
    "Platform", "Username", "User ID", "Display Name", "First Tweet Timestamp",
    "Tweet Text", "Tweet URL", "Likes", "Retweets", "Replies", "Quotes",
    "Bookmarks", "Views", "Tweet Type", "Conversation ID",
    "Subreddit", "Score", "Num Comments", "Post ID",
    "Image URLs"
]
# Mapping from Reddit data to our standard columns (approximate)
# Note: Many Twitter-specific fields don't directly map. Use placeholders or None.
# We'll use 'Tweet Text' for combined Title + Body, 'Conversation ID' for Post ID etc.

# --- Validate essential config --- #
if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT]):
    print("CRITICAL ERROR: Reddit API credentials (ID, Secret, User Agent) must be set in .env")
    exit()
if not GOOGLE_SHEETS_URL:
    print("CRITICAL ERROR: GOOGLE_SHEETS_URL must be set in .env")
    exit()
if 'YourRedditUsername' in REDDIT_USER_AGENT:
    print("CRITICAL ERROR: Please change 'YourRedditUsername' in REDDIT_USER_AGENT in your .env file to your actual Reddit username.")
    exit()

# --- Initialize Services --- #
reddit = None
worksheet_tgt = None
TARGET_TIMEZONE = pytz.utc

# Telegram Notification Function
def send_telegram_notification(message):
    """Sends a notification message to the configured Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # print("Telegram token or chat ID not configured. Skipping notification.")
        return
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4096
    truncated_message = message[:max_len] if len(message) > max_len else message
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': truncated_message}
    try:
        response = requests.post(send_url, json=payload, timeout=15)
        response.raise_for_status()
        # print(f"Sent Telegram notification: {truncated_message[:70].splitlines()[0]}...")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Telegram notification: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during Telegram notification sending: {e}")

# Reddit API
try:
    print("Connecting to Reddit API...")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        # Optional: Add username/password for user-specific actions, but read-only is fine here
        # username="YOUR_REDDIT_USERNAME",
        # password="YOUR_REDDIT_PASSWORD",
    )
    reddit.read_only = True # Explicitly set read-only mode
    print(f"Reddit API connected successfully. Read Only: {reddit.read_only}")
except Exception as e:
    error_msg = f"CRITICAL ERROR: Failed to connect to Reddit API: {e}"
    print(error_msg)
    send_telegram_notification(f"🚨 {error_msg}")
    exit()

# Google Sheets
try:
    print("Authenticating with Google...")
    SHEET_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file'
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    print(f"Opening Google Sheet: {GOOGLE_SHEETS_URL}")
    sh = gc.open_by_url(GOOGLE_SHEETS_URL)

    try:
        worksheet_tgt = sh.worksheet(TARGET_SHEET_NAME)
        print(f"Target sheet '{TARGET_SHEET_NAME}' found.")
        # We will append, so we don't need to clear unless desired
    except gspread.exceptions.WorksheetNotFound:
        print(f"Target sheet '{TARGET_SHEET_NAME}' not found. Creating it...")
        # Create sheet with standard columns
        worksheet_tgt = sh.add_worksheet(title=TARGET_SHEET_NAME, rows="1", cols=str(len(OUTPUT_COLUMNS)))
        worksheet_tgt.append_row(OUTPUT_COLUMNS, value_input_option='USER_ENTERED')
        print(f"Target sheet '{TARGET_SHEET_NAME}' created with headers.")

except Exception as e:
    error_msg = f"CRITICAL ERROR: Failed to authenticate or open/prepare Google Sheets: {e}"
    print(error_msg)
    send_telegram_notification(f"🚨 {error_msg}")
    exit()

# Timezone
try:
    TARGET_TIMEZONE = pytz.timezone(TARGET_TIMEZONE_STR)
except pytz.exceptions.UnknownTimeZoneError:
    print(f"Warning: Unknown timezone '{TARGET_TIMEZONE_STR}'. Defaulting to UTC.")
    # TARGET_TIMEZONE remains UTC

# --- Main Scraping Logic --- #
def scrape_reddit():
    """Fetches posts from target subreddits and appends them to the Google Sheet."""
    print("\n--- Starting Reddit Scrape ---")
    start_time = time.time()
    all_new_rows = []
    processed_post_ids = set() # Basic state to avoid duplicates within this run

    # Optional: Load existing post IDs from the target sheet to avoid duplicates across runs
    try:
        existing_data = worksheet_tgt.get_all_values()
        if len(existing_data) >= 2:
            df_existing = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            if 'Post ID' in df_existing.columns:
                processed_post_ids.update(df_existing['Post ID'].dropna().astype(str).tolist())
                print(f"Loaded {len(processed_post_ids)} existing post IDs from target sheet.")
    except Exception as e:
        print(f"Warning: Could not read existing data from target sheet to check for duplicates: {e}")


    for subreddit_name in TARGET_SUBREDDITS:
        print(f"\n--- Processing Subreddit: r/{subreddit_name} ---")
        subreddit_rows = 0
        try:
            subreddit = reddit.subreddit(subreddit_name)
            # Fetch top posts within the time filter
            # Other options: .hot(), .new(), .controversial()
            print(f"Fetching top {POST_LIMIT} posts from the last '{TIME_FILTER}'...")
            posts = subreddit.top(time_filter=TIME_FILTER, limit=POST_LIMIT)

            for post in posts:
                post_id = str(post.id)
                if post_id in processed_post_ids:
                    # print(f"  Skipping already processed post ID: {post_id}")
                    continue

                # Extract data
                title = post.title
                body = post.selftext
                author_obj = post.author
                author_name = str(author_obj) if author_obj else "[deleted]"
                author_id = str(author_obj.id) if author_obj else "[deleted]"
                timestamp_utc = datetime.datetime.fromtimestamp(post.created_utc, tz=pytz.utc)
                timestamp_local = timestamp_utc.astimezone(TARGET_TIMEZONE)
                timestamp_str = timestamp_local.strftime('%Y-%m-%d %H:%M:%S %Z%z')
                url = f"https://www.reddit.com{post.permalink}"
                score = post.score
                num_comments = post.num_comments
                subreddit_display = post.subreddit.display_name # Should match subreddit_name

                # Combine title and body for the main text field
                # Add context markers
                combined_text = f"Title: {title}"
                if body:
                    combined_text += f"\n\nBody:\n{body}"

                # Map to standard schema (approximations)
                row_data = {
                    "Platform": "Reddit",
                    "Username": author_name,
                    "User ID": author_id,
                    "Display Name": author_name,
                    "First Tweet Timestamp": timestamp_str,
                    "Tweet Text": combined_text,
                    "Tweet URL": url,
                    "Likes": 0,
                    "Retweets": 0,
                    "Replies": num_comments,
                    "Quotes": 0,
                    "Bookmarks": 0,
                    "Views": 0,
                    "Tweet Type": "Reddit Post",
                    "Conversation ID": post_id,
                    "Subreddit": subreddit_display,
                    "Score": score,
                    "Num Comments": num_comments,
                    "Post ID": post_id,
                    "Image URLs": "" # Assuming no image scraping for Reddit posts yet
                }

                # Ensure all columns are present, fill missing with empty string
                row_list = [str(row_data.get(col, '')) for col in OUTPUT_COLUMNS]
                all_new_rows.append(row_list)
                processed_post_ids.add(post_id) # Add to processed set for this run
                subreddit_rows += 1

            print(f"  Fetched {subreddit_rows} new posts from r/{subreddit_name}.")

        except praw.exceptions.PRAWException as e:
            print(f"  ERROR processing subreddit r/{subreddit_name}: {e}")
            send_telegram_notification(f"⚠️ Error processing subreddit r/{subreddit_name}: {e}")
        except Exception as e:
            print(f"  UNEXPECTED ERROR processing subreddit r/{subreddit_name}: {e}")
            import traceback
            tb_str = traceback.format_exc()
            send_telegram_notification(f"🚨 Unexpected Error processing r/{subreddit_name}: {e}\n```\n{tb_str[:1000]}\n```")
        # Add a small delay between subreddits to be polite to Reddit API
        time.sleep(2)

    # Append new rows to Google Sheet
    if all_new_rows:
        print(f"\nAppending {len(all_new_rows)} new rows to Google Sheet '{TARGET_SHEET_NAME}'...")
        try:
            worksheet_tgt.append_rows(all_new_rows, value_input_option='USER_ENTERED', table_range='A1')
            print("Successfully appended data.")
            end_time = time.time()
            duration = end_time - start_time
            success_msg = f"✅ Reddit Scraper finished successfully in {duration:.2f}s. Appended {len(all_new_rows)} new posts."
            print(success_msg)
            send_telegram_notification(success_msg)
        except Exception as e:
            error_msg = f"ERROR appending rows to target sheet '{TARGET_SHEET_NAME}': {e}"
            print(error_msg)
            import traceback
            tb_str = traceback.format_exc()
            send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:1000]}\n```")
    else:
        print("\nNo new Reddit posts found to append.")
        send_telegram_notification("ℹ️ Reddit Scraper run finished: No new posts found.")


# --- Run Main Logic --- #
if __name__ == "__main__":
    send_telegram_notification("🚀 Reddit scraper process starting...")
    try:
        scrape_reddit()
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
        send_telegram_notification("🛑 Reddit scraper stopped by user.")
    except Exception as main_e:
        error_msg = f"CRITICAL UNHANDLED ERROR in Reddit scraper main execution: {main_e}"
        print(error_msg)
        import traceback
        tb_str = traceback.format_exc()
        send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:3500]}\n``` Reddit scraper stopped.")

print("\nReddit Scraper finished.") 