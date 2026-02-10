import argparse
import csv
import logging
import sys
from datetime import date

from apify_client_wrapper import ApifyReelsScraper
from config import load_config
from data_processor import enrich_with_followers, filter_viral_reels
from sheets_exporter import export_to_sheets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_csv(csv_path: str) -> tuple[list[str], dict[str, int]]:
    """Read CSV with 'username' column and optional 'followers' column."""
    usernames = []
    followers_map = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if "username" not in (reader.fieldnames or []):
            logger.error("CSV must have a 'username' column. Found: %s", reader.fieldnames)
            sys.exit(1)

        for row in reader:
            username = row["username"].strip().lstrip("@")
            if not username:
                continue
            usernames.append(username)
            followers_str = row.get("followers", "").strip()
            if followers_str:
                try:
                    followers_map[username] = int(followers_str)
                except ValueError:
                    pass

    return usernames, followers_map


def main():
    parser = argparse.ArgumentParser(
        description="Instagram Viral Reels Parser — find viral reels among competitors"
    )
    parser.add_argument("--csv", required=True, help="Path to CSV file with competitor usernames")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    config = load_config(args.config)
    config.start_date = args.start_date
    config.end_date = args.end_date

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)

    if start > end:
        logger.error("start-date must be before end-date")
        sys.exit(1)

    logger.info("Period: %s — %s", start, end)

    # Read competitors
    usernames, csv_followers = parse_csv(args.csv)
    if not usernames:
        logger.error("No usernames found in CSV")
        sys.exit(1)

    logger.info("Loaded %d competitors: %s", len(usernames), ", ".join(usernames))

    # Init Apify client
    scraper = ApifyReelsScraper(config)

    # Fetch reels
    logger.info("Fetching reels from Apify...")
    reels = scraper.fetch_reels(usernames, start, end)
    logger.info("Fetched %d reels in date range", len(reels))

    if not reels:
        logger.warning("No reels found. Check usernames and date range.")
        return

    # Fetch follower counts for users without them in CSV
    users_without_followers = [u for u in usernames if u not in csv_followers]
    api_followers = {}
    if users_without_followers:
        logger.info("Fetching follower counts for %d users...", len(users_without_followers))
        api_followers = scraper.fetch_follower_counts(users_without_followers)

    # Enrich and filter
    enrich_with_followers(reels, api_followers, csv_followers)
    viral = filter_viral_reels(reels, config)

    logger.info(
        "Found %d viral reels (min views: %d, min ER: %.1f%%)",
        len(viral),
        config.min_views,
        config.min_engagement_rate,
    )

    if not viral:
        logger.info("No viral reels found matching the thresholds.")
        return

    # Top 5 preview
    logger.info("Top 5 viral reels:")
    for i, reel in enumerate(viral[:5], 1):
        logger.info(
            "  %d. @%s — %d views, ER %.2f%% — %s",
            i,
            reel.username,
            reel.views,
            reel.engagement_rate,
            reel.url,
        )

    # Export to Google Sheets
    logger.info("Exporting to Google Sheets...")
    sheet_url = export_to_sheets(viral, config)
    logger.info("Done! Spreadsheet: %s", sheet_url)


if __name__ == "__main__":
    main()
