from config import AppConfig
from models import ReelData


def calculate_engagement_rate(reel: ReelData) -> float:
    """ER = (likes + comments + shares) / followers * 100"""
    if reel.follower_count <= 0:
        return 0.0
    engagement = reel.likes + reel.comments + reel.shares
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


def filter_viral_reels(reels: list[ReelData], config: AppConfig) -> list[ReelData]:
    for reel in reels:
        reel.engagement_rate = calculate_engagement_rate(reel)

    viral = [
        reel
        for reel in reels
        if reel.views >= config.min_views
        and reel.engagement_rate >= config.min_engagement_rate
    ]
    viral.sort(key=lambda r: r.views, reverse=True)
    return viral
