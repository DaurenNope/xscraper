# Twitter Scraper to Google Sheets

This project scrapes tweets and replies from a specified list of Twitter users and appends new findings to a Google Sheet. It runs continuously, polling for new tweets periodically and using a state file to avoid duplicates.

## Features

*   Loads target usernames from `usernames.json`.
*   Loads Twitter account credentials (cookies recommended) from `accounts_config.json`.
*   Uses `twscrape` library for interacting with Twitter APIs.
*   Fetches recent tweets and replies (`limit` configurable via `.env`).
*   Tracks last seen tweet ID per user in `last_seen_ids.json` to only process new tweets ("listening mode").
*   Extracts tweet metadata (Likes, Retweets, Views, etc.) and categorizes tweet type (Original, Reply, Retweet, Quote).
*   Converts timestamps to a specified timezone.
*   Appends new, chronologically sorted tweet data to a specified Google Sheet.
*   Runs in a continuous loop with randomized sleep intervals between cycles.
*   Sends basic error notifications to a Telegram chat via a bot.
*   Configuration managed via a `.env` file.

## Setup

1.  **Clone/Create Project:** Ensure the project directory contains `scraper.py` and the necessary config files.
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Google Cloud Service Account:**
    *   Create a Google Cloud project.
    *   Enable the Google Sheets API and Google Drive API.
    *   Create a Service Account, grant it "Editor" role.
    *   Generate a JSON key file and save it as `service_account.json` in the project directory.
4.  **Google Sheet:**
    *   Create a new Google Sheet.
    *   Share the sheet with the service account's email address (found in Google Cloud Console), granting it "Editor" permissions.
    *   Copy the full URL of the Google Sheet.
5.  **Telegram Bot:**
    *   Talk to BotFather on Telegram to create a new bot.
    *   Copy the **Bot Token** provided.
    *   Find the **Chat ID** of the user or group where you want notifications sent (e.g., using `@userinfobot`).
6.  **Configure `.env`:**
    *   Create a `.env` file in the project root.
    *   Copy the content from the example below and **fill in your actual values** for `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `GOOGLE_SHEETS_URL`.
    *   Adjust other settings like timezone, delays, and sleep intervals if needed.
    ```dotenv
    # .env Example
    TELEGRAM_BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN_HERE"
    TELEGRAM_CHAT_ID="YOUR_TARGET_CHAT_ID_HERE"
    GOOGLE_SHEETS_URL="YOUR_GOOGLE_SHEETS_URL_HERE"
    SERVICE_ACCOUNT_FILE_PATH="service_account.json"
    TARGET_TIMEZONE="Asia/Almaty"
    DELAY_BETWEEN_USERS_SECONDS="15"
    ACCOUNTS_CONFIG_FILE="accounts_config.json"
    USERNAMES_FILE="usernames.json"
    STATE_FILE="last_seen_ids.json"
    TWEET_FETCH_LIMIT="30"
    BASE_SLEEP_INTERVAL_HOURS="4"
    RANDOM_SLEEP_RANGE_HOURS="1"
    ```
7.  **Configure `accounts_config.json`:**
    *   Create `accounts_config.json`.
    *   Add JSON objects for each Twitter account you want to use, including a unique `username` (for identification) and the `cookies` string. Dummy values can be used for password/email if using cookies.
    ```json
    // accounts_config.json Example
    [
      {
        "username": "AccountLabel1",
        "password": "dummy_pass",
        "email": "dummy@example.com",
        "email_password": "dummy_pw",
        "cookies": "auth_token=TOKEN_A; ct0=TOKEN_B",
        "proxy": null // Optional: "http://user:pass@host:port"
      },
      {
        "username": "AccountLabel2",
        "password": "dummy_pass2",
        "email": "dummy2@example.com",
        "email_password": "dummy_pw2",
        "cookies": "auth_token=TOKEN_X; ct0=TOKEN_Y",
        "proxy": null
      }
    ]
    ```
8.  **Configure `usernames.json`:**
    *   Create `usernames.json`.
    *   Add the target Twitter usernames (without `@`) in a JSON list under the `target_users` key.
    ```json
    // usernames.json Example
    {
      "target_users": [
        "username1",
        "username2"
      ]
    }
    ```

## Running the Scraper

Once configured, run the script from the project directory:

The script will run continuously in a loop, checking for new tweets at randomized intervals (based on `.env` settings).

To run it persistently on a server (even after disconnecting), use tools like `tmux`, `screen`, or configure it as a `systemd` service.

## Files

*   `scraper.py`: The main Python script.
*   `.env`: Stores configuration variables (API keys, URLs, settings). **DO NOT COMMIT TO GIT.**
*   `accounts_config.json`: Stores Twitter account credentials. **DO NOT COMMIT TO GIT.**
*   `service_account.json`: Google Cloud credentials. **DO NOT COMMIT TO GIT.**
*   `usernames.json`: List of target Twitter usernames to scrape.
*   `last_seen_ids.json`: Stores the ID of the last processed tweet for each user to prevent duplicates. **GENERATED AUTOMATICALLY.** Can be ignored by Git if desired.
*   `requirements.txt`: List of Python dependencies.
*   `.gitignore`: Specifies files/directories for Git to ignore.
*   `accounts.db` / `accounts.db-journal`: Database files generated by `twscrape` for session management. **GENERATED AUTOMATICALLY.** Ignored by Git.