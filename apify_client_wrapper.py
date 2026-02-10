from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from apify_client import ApifyClient

from config import AppConfig
from models import ReelData

logger = logging.getLogger(__name__)


class ApifyReelsScraper:
    def __init__(self, config: AppConfig):
        self.config = config
        self.client = ApifyClient(config.apify_token)

    def fetch_reels(
        self,
        usernames: list[str],
        start_date: date,
        end_date: date,
    ) -> list[ReelData]:
        logger.info("Starting Apify actor %s for %d users", self.config.actor_id, len(usernames))

        # Calculate relative date filter for onlyPostsNewerThan
        days_back = (date.today() - start_date).days
        if days_back < 1:
            days_back = 1

        actor_input = {
            "username": usernames,
            "resultsLimit": self.config.max_reels_per_profile,
            "onlyPostsNewerThan": f"{days_back} days",
        }

        logger.info("Actor input: %s", {k: v for k, v in actor_input.items()})

        run = self.client.actor(self.config.actor_id).call(run_input=actor_input)
        dataset_id = run["defaultDatasetId"]
        items = self.client.dataset(dataset_id).list_items().items

        logger.info("Received %d items from Apify", len(items))

        # Log all item types for debugging
        type_counts: dict[str, int] = {}
        for item in items:
            t = item.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        logger.info("Item types breakdown: %s", type_counts)

        reels = []
        for item in items:
            # Filter: only Video/Reel type (skip photos and carousels)
            item_type = item.get("type", "")
            if item_type not in ("Video", "Reel", "video", "reel"):
                logger.debug("Skipping non-reel item type: %s", item_type)
                continue

            reel = self._parse_item(item)
            if reel is None:
                continue
            # Additional date check for end_date (onlyPostsNewerThan only handles start)
            if reel.taken_at and reel.taken_at.date() > end_date:
                continue
            reels.append(reel)

        logger.info("After filtering (reels only, date range): %d reels", len(reels))
        return reels

    def fetch_follower_counts(self, usernames: list[str]) -> dict[str, int]:
        logger.info("Fetching follower counts for %d users via %s", len(usernames), self.config.profile_actor_id)

        actor_input = {
            "usernames": usernames,
        }

        run = self.client.actor(self.config.profile_actor_id).call(run_input=actor_input)
        dataset_id = run["defaultDatasetId"]
        items = self.client.dataset(dataset_id).list_items().items

        counts = {}
        for item in items:
            username = item.get("username", "")
            followers = item.get("followersCount", 0) or item.get("followers", 0)
            if username:
                counts[username] = followers

        logger.info("Got follower counts for %d users", len(counts))
        return counts

    def _parse_item(self, item: dict) -> Optional[ReelData]:
        try:
            # Post scraper uses "ownerUsername" primarily
            username = (
                item.get("ownerUsername", "")
                or item.get("username", "")
            )
            author = item.get("author", {}) or {}
            if not username:
                username = author.get("username", "")

            shortcode = item.get("shortCode", "") or item.get("code", "")
            url = item.get("url", "")
            if not url and shortcode:
                url = f"https://www.instagram.com/reel/{shortcode}/"

            taken_at = None
            timestamp = item.get("timestamp")
            if timestamp:
                if isinstance(timestamp, str):
                    taken_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif isinstance(timestamp, (int, float)):
                    taken_at = datetime.fromtimestamp(timestamp)

            views = item.get("videoPlayCount", 0) or item.get("videoViewCount", 0) or item.get("playsCount", 0) or item.get("viewsCount", 0) or 0
            likes = item.get("likesCount", 0) or item.get("likes", 0) or 0
            comments = item.get("commentsCount", 0) or item.get("comments", 0) or 0
            shares = item.get("sharesCount", 0) or 0
            caption = item.get("caption", "") or ""

            return ReelData(
                username=username,
                shortcode=shortcode,
                url=url,
                taken_at=taken_at,
                views=views,
                likes=likes,
                comments=comments,
                shares=shares,
                caption=caption,
            )
        except Exception as e:
            logger.warning("Failed to parse item: %s", e)
            return None
