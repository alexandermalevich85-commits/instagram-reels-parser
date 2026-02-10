from __future__ import annotations

import csv
import io
import json
import logging
import tempfile
from datetime import date, timedelta

import streamlit as st

from apify_client_wrapper import ApifyReelsScraper
from config import AppConfig
from data_processor import enrich_with_followers, filter_viral_reels
from sheets_exporter import export_to_sheets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

st.set_page_config(page_title="Instagram Viral Reels Parser", page_icon="🎬", layout="wide")
st.title("Instagram Viral Reels Parser")
st.caption("Find viral reels among your competitors")

# ── Sidebar: settings ──

st.sidebar.header("Settings")

# Apify token: from secrets or manual input
default_token = ""
try:
    default_token = st.secrets.get("APIFY_TOKEN", "")
except FileNotFoundError:
    pass

apify_token = st.sidebar.text_input("Apify API Token", value=default_token, type="password")

# Google Sheets service account
sa_json_file = st.sidebar.file_uploader("Google Service Account JSON", type=["json"])
use_secrets_sa = False
try:
    if "google_sheets" in st.secrets:
        use_secrets_sa = True
        st.sidebar.success("Google SA loaded from Streamlit secrets")
except FileNotFoundError:
    pass

st.sidebar.markdown("---")
st.sidebar.subheader("Thresholds")
min_views = st.sidebar.number_input("Min views", min_value=0, value=100_000, step=10_000)
min_er = st.sidebar.number_input("Min engagement rate (%)", min_value=0.0, value=3.0, step=0.5)

max_reels = st.sidebar.number_input("Max reels per profile", min_value=5, max_value=200, value=50, step=5)

st.sidebar.markdown("---")
st.sidebar.subheader("Google Sheets export")
spreadsheet_name = st.sidebar.text_input("Spreadsheet name", value="Viral Reels Report")

# ── Main area: inputs ──

col1, col2 = st.columns(2)

with col1:
    st.subheader("Competitors")
    input_mode = st.radio("Input mode", ["Type usernames", "Upload CSV"], horizontal=True)

    usernames_raw = ""
    csv_followers: dict[str, int] = {}

    if input_mode == "Type usernames":
        usernames_raw = st.text_area(
            "Usernames (one per line)",
            placeholder="cristiano\ntheweeknd\ninstagram",
            height=150,
        )
    else:
        uploaded_csv = st.file_uploader("Upload CSV (columns: username, followers)", type=["csv"])
        if uploaded_csv is not None:
            content = uploaded_csv.getvalue().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            lines = []
            for row in reader:
                username = row.get("username", "").strip().lstrip("@")
                if username:
                    lines.append(username)
                    f = row.get("followers", "").strip()
                    if f:
                        try:
                            csv_followers[username] = int(f)
                        except ValueError:
                            pass
            usernames_raw = "\n".join(lines)
            st.info(f"Loaded {len(lines)} usernames from CSV")

with col2:
    st.subheader("Date range")
    today = date.today()
    start_date = st.date_input("Start date", value=today - timedelta(days=30))
    end_date = st.date_input("End date", value=today)

# Parse usernames
usernames = [u.strip().lstrip("@") for u in usernames_raw.strip().splitlines() if u.strip()]

# ── Run button ──

st.markdown("---")

can_run = bool(apify_token and usernames and start_date <= end_date)

if not apify_token:
    st.warning("Enter your Apify API token in the sidebar.")
if not usernames:
    st.info("Add at least one competitor username.")
if start_date > end_date:
    st.error("Start date must be before end date.")

if st.button("Run parser", type="primary", disabled=not can_run):
    config = AppConfig(
        apify_token=apify_token,
        max_reels_per_profile=max_reels,
        min_views=min_views,
        min_engagement_rate=min_er,
        spreadsheet_name=spreadsheet_name,
    )

    scraper = ApifyReelsScraper(config)

    # Fetch reels
    try:
        with st.spinner(f"Fetching reels for {len(usernames)} accounts via Apify..."):
            reels = scraper.fetch_reels(usernames, start_date, end_date)
    except Exception as e:
        st.error(f"Apify error: {e}")
        st.stop()

    st.info(f"Fetched {len(reels)} reels in date range")

    if not reels:
        st.warning("No reels found. Check usernames and date range.")
        st.stop()

    # Fetch followers if needed
    users_without = [u for u in usernames if u not in csv_followers]
    api_followers: dict[str, int] = {}
    if users_without:
        try:
            with st.spinner(f"Fetching follower counts for {len(users_without)} users..."):
                api_followers = scraper.fetch_follower_counts(users_without)
        except Exception as e:
            st.warning(f"Could not fetch follower counts: {e}")

    enrich_with_followers(reels, api_followers, csv_followers)
    viral = filter_viral_reels(reels, config)

    st.success(f"Found **{len(viral)}** viral reels (min views: {min_views:,}, min ER: {min_er}%)")

    if not viral:
        st.info("No reels matched the thresholds. Try lowering the minimums.")
        st.stop()

    # Store results in session
    st.session_state["viral_reels"] = viral
    st.session_state["config"] = config

# ── Display results ──

if "viral_reels" in st.session_state:
    viral = st.session_state["viral_reels"]

    rows = []
    for r in viral:
        rows.append({
            "Username": r.username,
            "Followers": r.follower_count,
            "URL": r.url,
            "Date": r.taken_at.strftime("%Y-%m-%d %H:%M") if r.taken_at else "",
            "Views": r.views,
            "Likes": r.likes,
            "Comments": r.comments,
            "Shares": r.shares,
            "ER (%)": r.engagement_rate,
            "Caption": r.caption[:100],
        })

    st.dataframe(rows, use_container_width=True, hide_index=True)

    # CSV download
    csv_buffer = io.StringIO()
    if rows:
        writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    st.download_button(
        "Download CSV",
        data=csv_buffer.getvalue(),
        file_name="viral_reels.csv",
        mime="text/csv",
    )

    # Google Sheets export
    st.markdown("---")
    st.subheader("Export to Google Sheets")

    has_sa = sa_json_file is not None or use_secrets_sa
    if not has_sa:
        st.info("Upload a Google Service Account JSON in the sidebar, or configure it in Streamlit secrets.")

    if st.button("Export to Google Sheets", disabled=not has_sa):
        config = st.session_state.get("config", AppConfig())

        sa_file_path = None
        if sa_json_file is not None:
            # Write uploaded JSON to temp file
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w")
            sa_data = json.loads(sa_json_file.getvalue().decode("utf-8"))
            json.dump(sa_data, tmp)
            tmp.close()
            config.service_account_file = tmp.name
        elif use_secrets_sa:
            config.service_account_file = "__streamlit_secrets__"

        with st.spinner("Exporting to Google Sheets..."):
            url = export_to_sheets(viral, config)

        st.success(f"Exported! [Open spreadsheet]({url})")
