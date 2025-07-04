from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from .odds import Odds


class ArbitrageOpportunity(BaseModel):
    id: Optional[int] = None
    event_id: int
    sport_type: str
    home_team: str
    away_team: str
    event_date: datetime
    
    # Arbitrage details
    bet_type: str
    profit_percentage: float = Field(gt=0.0)
    total_stake: float = Field(gt=0.0)
    expected_profit: float = Field(gt=0.0)
    
    # Odds involved in arbitrage
    odds_data: List[Dict[str, Any]]  # List of odds with stake calculations
    
    # Metadata
    created_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    is_active: bool = True
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)
    
    # Risk assessment
    risk_level: str = "medium"  # low, medium, high
    notes: Optional[str] = None

    @validator('profit_percentage')
    def validate_profit_percentage(cls, v):
        if v <= 0:
            raise ValueError('Profit percentage must be positive')
        return round(v, 4)

    @validator('odds_data')
    def validate_odds_data(cls, v):
        if len(v) < 2:
            raise ValueError('Arbitrage requires at least 2 different odds')
        return v

    @classmethod
    def calculate_arbitrage(cls, odds_list: List[Odds], total_stake: float = 100.0) -> Optional['ArbitrageOpportunity']:
        """
        Calculate if there's an arbitrage opportunity from a list of odds
        """
        if len(odds_list) < 2:
            return None
        
        # Calculate implied probabilities
        total_implied_prob = sum(1 / odd.odds_decimal for odd in odds_list)
        
        # Check if arbitrage exists (total implied probability < 1)
        if total_implied_prob >= 1.0:
            return None
            
        profit_percentage = ((1 / total_implied_prob) - 1) * 100
        
        # Calculate optimal stakes for each bet
        odds_data = []
        for odd in odds_list:
            stake = (total_stake / odd.odds_decimal) / total_implied_prob
            potential_return = stake * odd.odds_decimal
            
            odds_data.append({
                'bookmaker_id': odd.bookmaker_id,
                'selection': odd.selection,
                'odds_decimal': odd.odds_decimal,
                'stake': round(stake, 2),
                'potential_return': round(potential_return, 2),
                'odds_id': odd.id
            })
        
        expected_profit = total_stake * (profit_percentage / 100)
        
        # Use the first odds item for event details
        first_odd = odds_list[0]
        
        return cls(
            event_id=first_odd.event_id,
            sport_type="unknown",  # This should be populated from event data
            home_team="",  # This should be populated from event data
            away_team="",  # This should be populated from event data
            event_date=datetime.now(),  # This should be populated from event data
            bet_type=first_odd.bet_type,
            profit_percentage=profit_percentage,
            total_stake=total_stake,
            expected_profit=expected_profit,
            odds_data=odds_data,
            confidence_score=min(odd.confidence_score for odd in odds_list)
        )

    def get_roi_percentage(self) -> float:
        """Return on Investment percentage"""
        return (self.expected_profit / self.total_stake) * 100

    def get_bookmaker_distribution(self) -> Dict[int, float]:
        """Get stake distribution by bookmaker"""
        distribution = {}
        for odds_item in self.odds_data:
            bookmaker_id = odds_item['bookmaker_id']
            stake = odds_item['stake']
            distribution[bookmaker_id] = distribution.get(bookmaker_id, 0) + stake
        return distribution