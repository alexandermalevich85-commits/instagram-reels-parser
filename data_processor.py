from config import AppConfig
from models import ReelData


def calculate_engagement_rate(reel: ReelData) -> float:
    """ER = (likes + comments) / followers * 100"""
    if reel.follower_count <= 0:
        return 0.0
    engagement = reel.likes + reel.comments
    return round((engagement / reel.follower_count) * 100, 2)


def enrich_with_followers(
    reels: list[ReelData],
    follower_counts: dict[str, int],
    csv_followers: dict[str, int],
) -> None:
    for reel in reels:
        if reel.follower_count > 0:
            continue
        reel.follower_count = (
            csv_followers.get(reel.username, 0)
            or follower_counts.get(reel.username, 0)
        )


def filter_viral_reels(reels: list[ReelData], config: AppConfig, is_posts: bool = False) -> list[ReelData]:
    for reel in reels:
        reel.engagement_rate = calculate_engagement_rate(reel)

    if is_posts:
        # Posts/Carousels: no views filter, sort by likes
        viral = [
            reel
            for reel in reels
            if reel.engagement_rate >= config.min_engagement_rate
        ]
        viral.sort(key=lambda r: r.likes, reverse=True)
    else:
        # Reels: filter by views, sort by views
        viral = [
            reel
            for reel in reels
            if reel.views >= config.min_views
            and reel.engagement_rate >= config.min_engagement_rate
        ]
        viral.sort(key=lambda r: r.views, reverse=True)
    return viral
