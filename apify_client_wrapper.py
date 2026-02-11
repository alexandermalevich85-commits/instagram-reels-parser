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
        return_raw: bool = False,
    ) -> list[ReelData] | tuple[list[ReelData], list[dict]]:
        logger.info("Starting Apify actor %s for %d users", self.config.actor_id, len(usernames))

        all_items: list[dict] = []

        # Fetch reels per user separately so resultsLimit applies per profile
        for username in usernames:
            actor_input = {
                "username": [username],
                "resultsLimit": self.config.max_reels_per_profile,
            }
            logger.info("Fetching reels for @%s (limit=%d)", username, self.config.max_reels_per_profile)

            run = self.client.actor(self.config.actor_id).call(run_input=actor_input)
            dataset_id = run["defaultDatasetId"]
            items = self.client.dataset(dataset_id).list_items().items
            logger.info("  @%s: received %d items", username, len(items))
            all_items.extend(items)

        logger.info("Total received: %d items from %d users", len(all_items), len(usernames))

        reels = []
        skipped_date = 0
        skipped_parse = 0
        for item in all_items:
            reel = self._parse_item(item)
            if reel is None:
                skipped_parse += 1
                continue
            # Client-side date filtering
            if reel.taken_at and not (start_date <= reel.taken_at.date() <= end_date):
                skipped_date += 1
                continue
            reels.append(reel)

        logger.info(
            "Filtering: %d reels in date range, %d outside range, %d failed to parse (from %d total)",
            len(reels), skipped_date, skipped_parse, len(all_items),
        )
        if return_raw:
            return reels, all_items
        return reels

    def fetch_posts(
        self,
        usernames: list[str],
        start_date: date,
        end_date: date,
        return_raw: bool = False,
    ) -> list[ReelData] | tuple[list[ReelData], list[dict]]:
        """Fetch posts/carousels via instagram-post-scraper (supports server-side date filter)."""
        post_actor = "apify/instagram-post-scraper"
        logger.info("Starting Apify actor %s for %d users", post_actor, len(usernames))

        all_items: list[dict] = []

        for username in usernames:
            actor_input = {
                "username": [username],
                "resultsLimit": self.config.max_reels_per_profile,
                "onlyPostsNewerThan": start_date.isoformat(),
            }
            logger.info("Fetching posts for @%s (limit=%d, since=%s)", username, self.config.max_reels_per_profile, start_date)

            run = self.client.actor(post_actor).call(run_input=actor_input)
            dataset_id = run["defaultDatasetId"]
            items = self.client.dataset(dataset_id).list_items().items
            logger.info("  @%s: received %d items", username, len(items))
            all_items.extend(items)

        logger.info("Total received: %d items from %d users", len(all_items), len(usernames))

        posts = []
        skipped_type = 0
        skipped_date = 0
        skipped_parse = 0
        for item in all_items:
            # Skip Video (reels) — keep only Sidecar (carousels) and Image (photos)
            item_type = item.get("type", "")
            if item_type in ("Video", "video"):
                skipped_type += 1
                continue

            parsed = self._parse_post_item(item)
            if parsed is None:
                skipped_parse += 1
                continue
            # Safety date check
            if parsed.taken_at and not (start_date <= parsed.taken_at.date() <= end_date):
                skipped_date += 1
                continue
            posts.append(parsed)

        logger.info(
            "Filtering: %d posts kept, %d skipped (video/reel), %d outside date, %d failed to parse (from %d total)",
            len(posts), skipped_type, skipped_date, skipped_parse, len(all_items),
        )
        if return_raw:
            return posts, all_items
        return posts

    def _parse_post_item(self, item: dict) -> Optional[ReelData]:
        """Parse a post/carousel item from instagram-post-scraper."""
        try:
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
                url = f"https://www.instagram.com/p/{shortcode}/"

            taken_at = None
            timestamp = item.get("timestamp")
            if timestamp:
                if isinstance(timestamp, str):
                    taken_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif isinstance(timestamp, (int, float)):
                    taken_at = datetime.fromtimestamp(timestamp)

            views = item.get("videoPlayCount", 0) or item.get("videoViewCount", 0) or 0
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
            logger.warning("Failed to parse post item: %s", e)
            return None

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
            author = item.get("author", {}) or {}
            username = (
                author.get("username", "")
                or item.get("ownerUsername", "")
                or item.get("username", "")
            )

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

            views = (
                item.get("playsCount", 0)
                or item.get("videoPlayCount", 0)
                or item.get("viewsCount", 0)
                or item.get("videoViewCount", 0)
                or 0
            )
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
            logger.warning("Failed to parse reel item: %s", e)
            return None
