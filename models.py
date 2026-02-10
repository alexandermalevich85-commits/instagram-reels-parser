from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ReelData(BaseModel):
    username: str
    follower_count: int = 0
    shortcode: str = ""
    url: str = ""
    taken_at: Optional[datetime] = None
    views: int = 0
    likes: int = 0
    comments: int = 0
    shares: int = 0
    engagement_rate: float = 0.0
    caption: str = ""
