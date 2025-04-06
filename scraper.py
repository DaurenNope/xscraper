import asyncio
from twscrape import API, gather
from twscrape.logger import set_log_level
import gspread
from google.oauth2.service_account import Credentials
import datetime
import pytz # Import pytz for timezone handling
import json # Import json for reading config file
import os   # Import os for environment variables
import time # Import time for sleep (though asyncio.sleep is used in async)
import random # Import random for randomized sleep
import requests # Import requests for Telegram notifications
from dotenv import load_dotenv # Import dotenv

# --- Load Environment Variables --- #
load_dotenv()

# --- Configuration from Environment Variables (with defaults) --- #
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
GOOGLE_SHEETS_URL = os.getenv('GOOGLE_SHEETS_URL')
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE_PATH', 'service_account.json')
TARGET_TIMEZONE_STR = os.getenv('TARGET_TIMEZONE', 'UTC')
ACCOUNTS_FILE = os.getenv('ACCOUNTS_CONFIG_FILE', 'accounts_config.json')
USERNAMES_FILE = os.getenv('USERNAMES_FILE', 'usernames.json')
STATE_FILE = os.getenv('STATE_FILE', 'last_seen_ids.json')
TWEET_FETCH_LIMIT = int(os.getenv('TWEET_FETCH_LIMIT', '30'))
DELAY_BETWEEN_USERS_SECONDS = int(os.getenv('DELAY_BETWEEN_USERS_SECONDS', '10'))
BASE_SLEEP_INTERVAL_HOURS = float(os.getenv('BASE_SLEEP_INTERVAL_HOURS', '4'))
RANDOM_SLEEP_RANGE_HOURS = float(os.getenv('RANDOM_SLEEP_RANGE_HOURS', '1'))

# Validate essential config
if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEETS_URL]):
    print("Error: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and GOOGLE_SHEETS_URL must be set in .env file.")
    exit()

# --- Telegram Notification Function --- #
def send_telegram_notification(message):
    """Sends a notification message to the configured Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram token or chat ID not configured. Skipping notification.")
        return

    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4096
    truncated_message = message[:max_len] if len(message) > max_len else message

    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': truncated_message,
        # 'parse_mode': 'Markdown' # Keep commented out for now
    }
    try:
        response = requests.post(send_url, json=payload, timeout=15)
        response.raise_for_status()
        print(f"Sent Telegram notification: {truncated_message[:70].splitlines()[0]}...")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Telegram notification: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during Telegram notification sending: {e}")


# --- Google Sheets Setup --- #
worksheet = None
try:
    print("Authenticating with Google...")
    SHEET_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file'
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SHEET_SCOPES)
    gc = gspread.authorize(creds)
    print("Opening Google Sheet...")
    sh = gc.open_by_url(GOOGLE_SHEETS_URL)
    worksheet = sh.sheet1
    print("Google Sheet opened successfully.")
except Exception as e:
    error_msg = f"CRITICAL ERROR: Failed to authenticate or open Google Sheet: {e}"
    print(error_msg)
    send_telegram_notification(f"🚨 {error_msg}")
    exit()

# --- Define Target Timezone --- #
try:
    TARGET_TIMEZONE = pytz.timezone(TARGET_TIMEZONE_STR)
except pytz.exceptions.UnknownTimeZoneError:
    print(f"Warning: Unknown timezone '{TARGET_TIMEZONE_STR}'. Defaulting to UTC.")
    TARGET_TIMEZONE = pytz.utc


# --- Helper Functions for State Management --- #
def load_last_seen_ids(filepath):
    """Loads the last seen tweet IDs from a JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"State file '{filepath}' not found, starting fresh.")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding state file '{filepath}'. Starting fresh.")
        send_telegram_notification(f"⚠️ WARNING: Corrupt state file '{filepath}'. Starting fresh.")
        return {}
    except Exception as e:
        print(f"Error loading state file '{filepath}': {e}. Starting fresh.")
        send_telegram_notification(f"⚠️ WARNING: Error loading state file '{filepath}': {e}. Starting fresh.")
        return {}

def save_last_seen_ids(filepath, state_data):
    """Saves the last seen tweet IDs to a JSON file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=4)
    except Exception as e:
        print(f"Error saving state to '{filepath}': {e}")
        send_telegram_notification(f"🚨 ERROR saving state to '{filepath}': {e}")

# --- Main Scraping Logic Function --- #
async def run_scrape_cycle(api, target_usernames_list, last_seen_state, initial_state_keys):
    """Runs a single cycle of fetching, processing, and saving tweets."""
    print(f"\n--- Starting scrape cycle at {datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")
    cycle_errors = [] # Collect non-critical errors for summary

    # --- Check Account Pool Status --- #
    # try:
    #     pool_status = await api.pool.get_status()
    #     active_accounts = [acc for acc in pool_status if acc.get('logged_in')] # Check for 'logged_in': True
    #     if not active_accounts:
    #         error_msg = "CRITICAL: No active/logged-in Twitter accounts found in the pool. Please refresh cookies."
    #         print(error_msg)
    #         send_telegram_notification(f"🚨 {error_msg}")
    #         return False # Indicate cycle failed due to no active accounts
    #     else:
    #         print(f"  {len(active_accounts)} active account(s) available in the pool.")
    # except Exception as status_err:
    #     # error_msg = f"ERROR checking account pool status: {status_err}" # Hide the error for now as the method is wrong
    #     # print(error_msg)
    #     # cycle_errors.append(error_msg)
    #     pass # Ignore the status check error for now
    # --- End Check Account Pool Status --- #

    # --- Prepare Headers (Run only if needed within the cycle) --- #
    header = [
        "Username", "User ID", "Display Name", "Tweet Timestamp", "Tweet Text", "Tweet URL",
        "Likes", "Retweets", "Replies", "Quotes", "Bookmarks", "Views",
        "Tweet Type", "Conversation ID"
    ]
    try:
        current_header = worksheet.row_values(1)
        if current_header != header:
             worksheet.insert_row(header, 1)
             print("Added/Corrected header row in Google Sheet.")
    except gspread.exceptions.APIError as api_err:
         print(f"Google Sheets API error checking/writing header: {api_err}")
         cycle_errors.append(f"Google Sheets API error checking/writing header: {api_err}")
    except Exception as inner_e:
         print(f"Failed to check/write header: {inner_e}")
         cycle_errors.append(f"ERROR: Failed during header check/write in Google Sheet: {inner_e}")

    print(f"Fetching details for {len(target_usernames_list)} usernames...")
    all_rows_to_append = []
    processed_tweet_ids_this_run = set()

    for username in target_usernames_list:
        print(f"--- Processing @{username} ---")
        user_display_name = "N/A"
        user_id_str = "N/A"
        last_seen_id = last_seen_state.get(username, 0)
        # print(f"  Last seen tweet ID for {username}: {last_seen_id}") # Verbose

        try:
            # Fetch User Profile
            user = await api.user_by_login(username)
            if user:
                user_id_str = str(user.id)
                # print(f"  User ID: {user_id_str}")
                user_data = user.dict()
                user_display_name = user_data.get('displayname', 'N/A')
                # print(f"  Display Name: {user_display_name}")

                # Fetch User's Recent Tweets & Replies
                # print(f"  Fetching recent tweets and replies for @{username} (User ID: {user_id_str})...")
                try:
                    fetched_tweets = await gather(api.user_tweets_and_replies(user.id, limit=TWEET_FETCH_LIMIT))
                    new_tweets = [t for t in fetched_tweets if t.id > last_seen_id]

                    if new_tweets:
                        print(f"  Found {len(new_tweets)} new tweet(s) (out of {len(fetched_tweets)} fetched).")
                        new_tweets.reverse()
                        max_new_id = last_seen_id

                        for tweet in new_tweets:
                            if tweet.id > max_new_id:
                                max_new_id = tweet.id
                            if tweet.id in processed_tweet_ids_this_run:
                                print(f"    Skipping duplicate tweet ID {tweet.id} within this run.")
                                continue
                            processed_tweet_ids_this_run.add(tweet.id)

                            # Process tweet data
                            utc_time = tweet.date
                            local_time = utc_time.astimezone(TARGET_TIMEZONE)
                            tweet_timestamp = local_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
                            tweet_text = tweet.rawContent
                            tweet_url = tweet.url
                            tweet_type = "Original Tweet"
                            if tweet.retweetedTweet: tweet_type = "Retweet"
                            elif tweet.quotedTweet: tweet_type = "Quote Tweet"
                            elif tweet.inReplyToTweetId: tweet_type = "Reply"
                            likes = tweet.likeCount or 0
                            retweets = tweet.retweetCount or 0
                            replies = tweet.replyCount or 0
                            quotes = tweet.quoteCount or 0
                            bookmarks = tweet.bookmarkedCount or 0
                            views = tweet.viewCount or 0
                            conversation_id_str = str(tweet.conversationId) if tweet.conversationId else "N/A"

                            row = [
                                username, user_id_str, user_display_name, tweet_timestamp,
                                tweet_text, tweet_url, likes, retweets, replies, quotes,
                                bookmarks, views, tweet_type, conversation_id_str
                            ]
                            all_rows_to_append.append(row)

                        # Update state
                        if max_new_id > last_seen_id:
                            last_seen_state[username] = max_new_id
                            print(f"  Updated last seen ID for {username} to {max_new_id}")

                    else:
                        # print(f"  No new tweets found for {username} since ID {last_seen_id}.") # Verbose
                        # Initialize state for new users
                        if username not in initial_state_keys and fetched_tweets:
                             latest_fetched_id = max((t.id for t in fetched_tweets), default=0)
                             if latest_fetched_id > last_seen_id:
                                 last_seen_state[username] = latest_fetched_id
                                 print(f"  Initialized last seen ID for new user {username} to {latest_fetched_id}")

                except Exception as e:
                    error_msg = f"ERROR fetching/processing tweets for @{username}: {e}"
                    print(f"  {error_msg}")
                    cycle_errors.append(error_msg)

            else:
                print(f"  Could not find user @{username}")
                # Optionally track users consistently not found

        except Exception as e:
            error_msg = f"ERROR fetching user profile @{username}: {e}"
            print(f"  {error_msg}")
            cycle_errors.append(error_msg)

        # --- Delay Between Users --- #
        # print(f"Waiting {DELAY_BETWEEN_USERS_SECONDS} seconds...") # Verbose
        await asyncio.sleep(DELAY_BETWEEN_USERS_SECONDS)

    # --- Sort collected rows --- #
    if all_rows_to_append:
        print("\nSorting collected rows by timestamp...")
        try:
            all_rows_to_append.sort(key=lambda row_item: row_item[3])
            print("Sorting complete.")
        except Exception as sort_e:
            error_msg = f"ERROR sorting tweet data: {sort_e}. Appending unsorted data."
            print(f"{error_msg}")
            cycle_errors.append(error_msg)

    # --- Append to Google Sheet --- #
    if all_rows_to_append:
        print(f"\nAppending {len(all_rows_to_append)} new rows to Google Sheet...")
        try:
            worksheet.append_rows(all_rows_to_append, value_input_option='USER_ENTERED')
            print("Successfully appended data to Google Sheet.")
        except gspread.exceptions.APIError as api_err:
            error_msg = f"Google Sheets API error appending rows: {api_err}"
            print(error_msg)
            cycle_errors.append(error_msg)
        except Exception as e:
            error_msg = f"An unexpected error occurred during sheet append: {e}"
            print(error_msg)
            cycle_errors.append(error_msg)
    else:
        print("\nNo new tweet data collected to append to Google Sheet in this cycle.")

    # --- Save Updated State --- #
    save_last_seen_ids(STATE_FILE, last_seen_state)
    # print(f"Updated state saved to {STATE_FILE}") # Verbose

    # --- Notify about errors during the cycle --- #
    if cycle_errors:
        error_summary = f"⚠️ {len(cycle_errors)} error(s) occurred during the scrape cycle:\n"
        error_summary += "\n".join([f"- {err}" for err in cycle_errors[:5]]) # Show first 5 errors
        if len(cycle_errors) > 5:
            error_summary += f"\n- ... and {len(cycle_errors) - 5} more."
        send_telegram_notification(error_summary)

    print(f"--- Scrape cycle finished at {datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')} ---")
    return True # Indicate cycle completed (even with non-critical errors)


# --- Main Execution Loop --- #
async def main():
    api = None # Initialize api to None
    accounts_loaded_successfully = False

    # --- Load Twitter Accounts (Once at Startup) ---
    try:
        api = API() # Initialize API here
        accounts_added_count = 0
        print(f"Loading Twitter accounts from {ACCOUNTS_FILE}...")
        with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
            accounts_data = json.load(f)
            if not isinstance(accounts_data, list):
                raise ValueError(f"Expected a list of accounts, but got {type(accounts_data)}")

            for account in accounts_data:
                acc_username = account.get('username')
                acc_password = account.get('password')
                acc_email = account.get('email')
                acc_email_password = account.get('email_password')
                acc_cookies = account.get('cookies')
                acc_proxy = account.get('proxy')

                if not acc_username or not acc_cookies:
                    print(f"Warning: Skipping account in {ACCOUNTS_FILE} due to missing username or cookies.")
                    continue
                try:
                    await api.pool.add_account(
                        acc_username, acc_password, acc_email, acc_email_password,
                        cookies=acc_cookies, proxy=acc_proxy
                    )
                    accounts_added_count += 1
                except Exception as add_err:
                    print(f"Error adding account '{acc_username}' from config: {add_err}")
                    # Decide if this should be fatal or just a warning

            if accounts_added_count == 0:
                raise ValueError("No valid accounts were loaded and added from config.")
            else:
                print(f"Successfully loaded and added {accounts_added_count} account(s) to the pool.")
                accounts_loaded_successfully = True

    except FileNotFoundError:
        error_msg = f"CRITICAL ERROR: Accounts file '{ACCOUNTS_FILE}' not found."
        print(error_msg)
        send_telegram_notification(f"🚨 {error_msg} Scraper stopping.")
        return
    except json.JSONDecodeError:
        error_msg = f"CRITICAL ERROR: Could not decode JSON from accounts file '{ACCOUNTS_FILE}'."
        print(error_msg)
        send_telegram_notification(f"🚨 {error_msg} Scraper stopping.")
        return
    except Exception as e:
        error_msg = f"CRITICAL ERROR loading accounts from {ACCOUNTS_FILE}: {e}"
        print(error_msg)
        send_telegram_notification(f"🚨 {error_msg} Scraper stopping.")
        return # Stop if accounts can't be loaded

    if not accounts_loaded_successfully or not api:
         error_msg = "CRITICAL ERROR: Failed to initialize API or load accounts."
         print(error_msg)
         send_telegram_notification(f"🚨 {error_msg} Scraper stopping.")
         return


    # --- Main Loop ---
    while True:
        cycle_start_time = time.monotonic()
        target_usernames_list = []
        last_seen_state = {}
        initial_state_keys = set()
        cycle_completed_successfully = False

        try:
            # --- Load Usernames and State ---
            print(f"\nLoading configuration for cycle...")
            with open(USERNAMES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                target_usernames_list = data.get('target_users', [])
                if not target_usernames_list:
                    print(f"Warning: No usernames found in {USERNAMES_FILE}. Sleeping until next cycle.")
                else:
                     print(f"Loaded {len(target_usernames_list)} target usernames.")

            if target_usernames_list:
                last_seen_state = load_last_seen_ids(STATE_FILE)
                initial_state_keys = set(last_seen_state.keys())

                # --- Run Scrape Cycle ---
                cycle_completed_successfully = await run_scrape_cycle(api, target_usernames_list, last_seen_state, initial_state_keys)

            else:
                # No users to process, just sleep
                print("No target users loaded. Skipping scrape cycle.")
                cycle_completed_successfully = True # Mark as 'success' as there was nothing to do

        except FileNotFoundError as fnf_err:
            error_msg = f"ERROR: Usernames file '{USERNAMES_FILE}' not found: {fnf_err}. Skipping cycle."
            print(error_msg)
            send_telegram_notification(f"⚠️ {error_msg}")
        except json.JSONDecodeError as json_err:
             error_msg = f"ERROR: Could not decode JSON from '{USERNAMES_FILE}': {json_err}. Skipping cycle."
             print(error_msg)
             send_telegram_notification(f"⚠️ {error_msg}")
        except Exception as cycle_e:
            # Catch unexpected errors during the whole cycle (incl. loading files)
            error_msg = f"CRITICAL ERROR during scrape cycle execution: {cycle_e}"
            print(error_msg)
            import traceback
            tb_str = traceback.format_exc()
            send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:3500]}\n```") # Send traceback snippet

        # --- Calculate Randomized Sleep Interval --- #
        cycle_end_time = time.monotonic()
        cycle_duration_seconds = cycle_end_time - cycle_start_time
        if cycle_completed_successfully: # Only print cycle time if it ran
             print(f"Scrape cycle duration: {cycle_duration_seconds:.2f} seconds.")

        base_interval_seconds = BASE_SLEEP_INTERVAL_HOURS * 3600
        random_range_seconds = RANDOM_SLEEP_RANGE_HOURS * 3600
        min_sleep = max(60, base_interval_seconds - random_range_seconds)
        max_sleep = base_interval_seconds + random_range_seconds
        target_sleep = random.uniform(min_sleep, max_sleep)
        # Ensure sleep is positive even if cycle took longer than min sleep interval
        actual_sleep = max(60.0, target_sleep - cycle_duration_seconds)
        sleep_minutes = actual_sleep / 60

        print(f"\nSleeping for {sleep_minutes:.2f} minutes until next cycle...")
        await asyncio.sleep(actual_sleep)


if __name__ == "__main__":
    # set_log_level("DEBUG") # Uncomment for more detailed twscrape logs
    send_telegram_notification("🚀 Twitter scraper process starting...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
        send_telegram_notification("🛑 Twitter scraper stopped by user.")
    except Exception as main_e:
        error_msg = f"CRITICAL UNHANDLED ERROR in main execution: {main_e}"
        print(error_msg)
        import traceback
        tb_str = traceback.format_exc()
        send_telegram_notification(f"🚨 {error_msg}\n```\n{tb_str[:3500]}\n``` Scraper stopped.") 