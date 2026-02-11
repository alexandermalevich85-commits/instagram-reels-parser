from __future__ import annotations

import logging

import gspread

from config import AppConfig
from models import ReelData

logger = logging.getLogger(__name__)


def _get_gspread_client(config: AppConfig) -> gspread.Client:
    """Authenticate with gspread via file or Streamlit secrets."""
    if config.service_account_file == "__streamlit_secrets__":
        import streamlit as st

        creds_dict = dict(st.secrets["google_sheets"])
        return gspread.service_account_from_dict(creds_dict)
    return gspread.service_account(filename=config.service_account_file)


def export_to_sheets(reels: list[ReelData], config: AppConfig, is_posts: bool = False) -> str:
    gc = _get_gspread_client(config)

    try:
        spreadsheet = gc.open(config.spreadsheet_name)
        logger.info("Opened existing spreadsheet: %s", config.spreadsheet_name)
    except gspread.SpreadsheetNotFound:
        spreadsheet = gc.create(config.spreadsheet_name)
        logger.info("Created new spreadsheet: %s", config.spreadsheet_name)

    sheet_name = "Posts" if is_posts else "Reels"
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=len(reels) + 1,
            cols=10,
        )

    if is_posts:
        headers = ["Username", "Followers", "URL", "Date", "Likes", "Comments", "ER (%)", "Caption"]
        rows = [headers]
        for reel in reels:
            rows.append([
                reel.username,
                reel.follower_count,
                reel.url,
                reel.taken_at.strftime("%Y-%m-%d %H:%M") if reel.taken_at else "",
                reel.likes,
                reel.comments,
                reel.engagement_rate,
                reel.caption,
            ])
    else:
        headers = ["Username", "Followers", "URL", "Date", "Views", "Likes", "Comments", "ER (%)", "Caption"]
        rows = [headers]
        for reel in reels:
            rows.append([
                reel.username,
                reel.follower_count,
                reel.url,
                reel.taken_at.strftime("%Y-%m-%d %H:%M") if reel.taken_at else "",
                reel.views,
                reel.likes,
                reel.comments,
                reel.engagement_rate,
                reel.caption,
            ])

    worksheet.update(range_name="A1", values=rows)

    url = spreadsheet.url
    logger.info("Exported %d items to Google Sheets: %s", len(reels), url)
    return url
