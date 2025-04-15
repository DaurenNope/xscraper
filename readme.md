# Multi-Platform Content Analyzer and Rewriter

This project reads raw content scraped from different platforms (currently Reddit, potentially Twitter), analyzes it, filters relevant items, rewrites them using the Gemini API in English and Russian with a specific brand voice (Rahmet Labs), and saves the results to a local CSV file and a Google Sheet.

## Features

*   Processes data scraped from specified Google Sheet tabs (`REDDIT_SOURCE_SHEET_NAMES` or `TWITTER_SOURCE_SHEET_NAMES` in `.env`).
*   Handles platform-specific data processing (e.g., identifying Reddit posts).
*   Filters content based on:
    *   Already processed items (using URLs from local state and target Google Sheet).
    *   Content type (e.g., 'Reddit Post', 'Original Tweet', 'Thread').
    *   Minimum content length.
    *   Presence of relevant keywords.
    *   Exclusion of overly structured prompts/code.
*   Uses Google Gemini API (`gemini-1.5-flash` by default) to rewrite filtered content into English and Russian.
*   Applies a specific "Rahmet Labs" brand voice (direct, practical, no hype) during rewriting.
*   Manages API calls with concurrency limits and backoff/retry logic.
*   Saves processed and rewritten data incrementally to a platform-specific local CSV file (`REDDIT_LOCAL_STATE_FILE` or `TWITTER_LOCAL_STATE_FILE`).
*   Syncs the final results from the local CSV to a specified target Google Sheet tab (`REDDIT_ANALYZED_SHEET_NAME` or `TWITTER_ANALYZED_SHEET_NAME`).
*   Sends start/stop/error notifications to a Telegram chat.
*   Configuration managed via a `.env` file.
*   Selects platform via command-line argument (`--platform`).

## Setup

1.  **Clone/Create Project:** Ensure the project directory contains `analyzer.py`, `requirements.txt`, and the necessary example config files.
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Google Cloud Service Account:**
    *   Create a Google Cloud project.
    *   Enable the Google Sheets API and Google Drive API.
    *   Create a Service Account, grant it "Editor" role.
    *   Generate a JSON key file.
    *   Copy `service_account.json.example` to `service_account.json`.
    *   Paste the contents of your downloaded JSON key file into `service_account.json`. **Ensure `service_account.json` is listed in your `.gitignore` file.**
4.  **Google Sheet:**
    *   Create a single Google Sheet to hold both raw scraped data and analyzed results.
    *   **Raw Data Sheets:** Create sheets for your raw scraped data (e.g., `Sheet_Reddit_Raw`). The names must match those configured in `.env`.
    *   **Analyzed Sheets:** The script will automatically create target sheets (e.g., `Analyzed_Reddit`) if they don't exist.
    *   Share the entire Google Sheet file with the service account's email address (found in `client_email` of `service_account.json`), granting it "Editor" permissions.
    *   Copy the full URL of the Google Sheet.
5.  **Telegram Bot:**
    *   Talk to BotFather on Telegram to create a new bot.
    *   Copy the **Bot Token** provided.
    *   Find the **Chat ID** of the user or group where you want notifications sent (e.g., using `@userinfobot`).
6.  **Gemini API Key:**
    *   Obtain an API key for Google Gemini (e.g., via Google AI Studio).
7.  **Configure `.env`:**
    *   Copy `.env.example` to `.env`.
    *   Open the `.env` file and **fill in your actual values** for:
        *   `TELEGRAM_BOT_TOKEN`
        *   `TELEGRAM_CHAT_ID`
        *   `GOOGLE_SHEETS_URL` (URL of the single sheet containing raw and analyzed data)
        *   `GEMINI_API_KEY`
        *   `SERVICE_ACCOUNT_FILE_PATH` (defaults to `service_account.json`, change if needed)
    *   **Configure Platform-Specific Settings:**
        *   `REDDIT_SOURCE_SHEET_NAMES`: Comma-separated names of sheets containing raw Reddit data (e.g., `Sheet_Reddit_Raw`).
        *   `REDDIT_ANALYZED_SHEET_NAME`: Name for the target sheet for analyzed Reddit data (e.g., `Analyzed_Reddit`).
        *   `REDDIT_LOCAL_STATE_FILE`: Filename for the local CSV storing processed Reddit data (e.g., `reddit_processed_state.csv`).
        *   *(Add similar `TWITTER_...` variables if/when Twitter processing is added)*
    *   Adjust other settings like `TARGET_TIMEZONE`, `GEMINI_CONCURRENT_REQUESTS` if needed.
    *   **Ensure `.env` is listed in your `.gitignore` file.**

## Running the Analyzer

Once configured, run the script from the project directory, specifying the platform:

**For Reddit:**
```bash
python analyzer.py --platform reddit
```

**For Twitter (if implemented):**
```bash
# python analyzer.py --platform twitter
```

The script will perform the analysis, filtering, rewriting, and syncing steps, then exit. It is designed to be run manually or via a scheduler (like cron).

## Files

*   `analyzer.py`: The main Python script for processing and rewriting.
*   `scrapers/`: Directory containing platform-specific scraping scripts (e.g., `reddit_scraper.py`). These are run separately to populate the raw data sheets.
*   `.env`: Stores configuration variables (API keys, URLs, settings). **DO NOT COMMIT TO GIT.**
*   `service_account.json`: Google Cloud credentials. **DO NOT COMMIT TO GIT.**
*   `requirements.txt`: List of Python dependencies.
*   `.gitignore`: Specifies files/directories for Git to ignore.
*   `*_processed_state.csv`: Local CSV files storing processed data (e.g., `reddit_processed_state.csv`). **GENERATED AUTOMATICALLY.** Should be ignored by Git.
*   `.env.example`: Example structure for the `.env` file.
*   `service_account.json.example`: Example structure/placeholder for Google credentials.
*   `README.md`: This file.
*   *(Other config files like `accounts_config.json`, `usernames.json` might be used by specific scrapers)*