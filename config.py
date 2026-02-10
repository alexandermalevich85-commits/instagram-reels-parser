from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class AppConfig:
    # Apify
    apify_token: str = ""
    actor_id: str = "apify/instagram-reel-scraper"
    profile_actor_id: str = "apify/instagram-profile-scraper"
    max_reels_per_profile: int = 50

    # Thresholds
    min_views: int = 100_000
    min_engagement_rate: float = 3.0

    # Google Sheets
    service_account_file: str = "service_account.json"
    spreadsheet_name: str = "Viral Reels Report"
    worksheet_name: str = "Reels"

    # Date range (set from CLI)
    start_date: str = ""
    end_date: str = ""


def load_config(config_path: str = "config.yaml") -> AppConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    apify = raw.get("apify", {})
    thresholds = raw.get("thresholds", {})
    sheets = raw.get("google_sheets", {})

    return AppConfig(
        apify_token=apify.get("token", ""),
        actor_id=apify.get("actor_id", "apify/instagram-reel-scraper"),
        profile_actor_id=apify.get("profile_actor_id", "apify/instagram-profile-scraper"),
        max_reels_per_profile=apify.get("max_reels_per_profile", 50),
        min_views=thresholds.get("min_views", 100_000),
        min_engagement_rate=thresholds.get("min_engagement_rate", 3.0),
        service_account_file=sheets.get("service_account_file", "service_account.json"),
        spreadsheet_name=sheets.get("spreadsheet_name", "Viral Reels Report"),
        worksheet_name=sheets.get("worksheet_name", "Reels"),
    )
