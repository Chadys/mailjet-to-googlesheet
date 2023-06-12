import datetime
from dataclasses import dataclass, field


@dataclass
class Campaign:
    id: int
    title: str


@dataclass
class SheetState:
    has_headers: bool | None = None
    last_campaign_date: datetime.date = None
    campaign_period: str = "Year"
    link_mapping: dict = field(default_factory=dict)
    user_agent_mapping: dict = field(default_factory=dict)
    region_mapping: dict = field(default_factory=dict)
    new_rows = {"campaign": 2, "link": 2, "user_agent": 2, "region": 2}
