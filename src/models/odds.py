from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
from enum import Enum


class SportType(str, Enum):
    FOOTBALL = "football"
    RUGBY = "rugby"
    CRICKET = "cricket"
    TENNIS = "tennis"
    BASKETBALL = "basketball"
    SOCCER = "soccer"
    HORSE_RACING = "horse_racing"


class BetType(str, Enum):
    MATCH_WINNER = "match_winner"
    OVER_UNDER = "over_under"
    HANDICAP = "handicap"
    BOTH_TEAMS_TO_SCORE = "both_teams_to_score"
    CORRECT_SCORE = "correct_score"
    FIRST_GOAL_SCORER = "first_goal_scorer"


class BookmakerEnum(str, Enum):
    HOLLYWOODBETS = "hollywoodbets"
    BETWAY = "betway"
    SUPABETS = "supabets"
    PLAYABETS = "playabets"
    PLAYBETS = "playbets"
    YESBET = "yesbet"


class Bookmaker(BaseModel):
    id: Optional[int] = None
    name: BookmakerEnum
    display_name: str
    website_url: str
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        use_enum_values = True


class SportEvent(BaseModel):
    id: Optional[int] = None
    sport_type: SportType
    home_team: str
    away_team: str
    event_date: datetime
    league: str
    country: str = "South Africa"
    is_live: bool = False
    external_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @validator('home_team', 'away_team')
    def validate_teams(cls, v):
        if not v or len(v.strip()) < 2:
            raise ValueError('Team name must be at least 2 characters')
        return v.strip()

    class Config:
        use_enum_values = True


class Odds(BaseModel):
    id: Optional[int] = None
    event_id: int
    bookmaker_id: int
    bet_type: BetType
    selection: str  # e.g., "Home", "Away", "Draw", "Over 2.5", etc.
    odds_decimal: float = Field(gt=1.0, le=1000.0)
    odds_fractional: Optional[str] = None
    odds_american: Optional[int] = None
    stake_limit: Optional[float] = None
    is_available: bool = True
    last_updated: datetime
    created_at: Optional[datetime] = None
    
    # Additional metadata
    market_id: Optional[str] = None
    original_data: Optional[Dict[str, Any]] = None
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)

    @validator('odds_decimal')
    def validate_odds(cls, v):
        if v <= 1.0:
            raise ValueError('Decimal odds must be greater than 1.0')
        return round(v, 3)

    @property
    def implied_probability(self) -> float:
        """Calculate implied probability from decimal odds"""
        return 1 / self.odds_decimal

    @property
    def margin_percentage(self) -> float:
        """Calculate bookmaker margin"""
        return (self.implied_probability - 1) * 100

    class Config:
        use_enum_values = True