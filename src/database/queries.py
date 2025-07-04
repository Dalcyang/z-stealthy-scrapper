from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


class OddsQueries:
    """Helper class for odds-related database queries"""
    
    def __init__(self, supabase_manager):
        self.db = supabase_manager
    
    async def get_odds_by_event_and_bookmaker(self, event_id: int, bookmaker_id: int) -> List[Dict[str, Any]]:
        """Get all odds for a specific event and bookmaker"""
        return await self.db.execute_query(
            'odds', 'select',
            filters={
                'event_id': event_id,
                'bookmaker_id': bookmaker_id,
                'is_available': True
            }
        )
    
    async def get_latest_odds_by_sport(self, sport_type: str, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get latest odds for a specific sport type"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        return await self.db.execute_query(
            'odds', 'select',
            filters={
                'last_updated': {'gte': cutoff_time.isoformat()},
                'is_available': True
            }
        )
    
    async def get_odds_comparison(self, event_id: int, bet_type: str) -> List[Dict[str, Any]]:
        """Get odds comparison across all bookmakers for a specific event and bet type"""
        return await self.db.execute_query(
            'odds', 'select',
            filters={
                'event_id': event_id,
                'bet_type': bet_type,
                'is_available': True
            }
        )
    
    async def get_bookmaker_statistics(self, bookmaker_id: int, days: int = 7) -> Dict[str, Any]:
        """Get statistics for a specific bookmaker"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Get total odds count
        odds_data = await self.db.execute_query(
            'odds', 'select',
            filters={
                'bookmaker_id': bookmaker_id,
                'created_at': {'gte': cutoff_date.isoformat()}
            }
        )
        
        if not odds_data:
            return {
                'total_odds': 0,
                'avg_odds': 0,
                'unique_events': 0,
                'last_update': None
            }
        
        # Calculate statistics
        odds_values = [float(odd['odds_decimal']) for odd in odds_data]
        unique_events = len(set(odd['event_id'] for odd in odds_data))
        last_update = max(odd['last_updated'] for odd in odds_data) if odds_data else None
        
        return {
            'total_odds': len(odds_data),
            'avg_odds': sum(odds_values) / len(odds_values) if odds_values else 0,
            'unique_events': unique_events,
            'last_update': last_update
        }


class ArbitrageQueries:
    """Helper class for arbitrage-related database queries"""
    
    def __init__(self, supabase_manager):
        self.db = supabase_manager
    
    async def get_top_arbitrage_opportunities(self, limit: int = 10, min_profit: float = 1.0) -> List[Dict[str, Any]]:
        """Get top arbitrage opportunities by profit percentage"""
        return await self.db.execute_query(
            'arbitrage_opportunities', 'select',
            filters={
                'is_active': True,
                'profit_percentage': {'gte': min_profit},
                'expires_at': {'gte': datetime.now().isoformat()}
            }
        )
    
    async def get_arbitrage_by_sport(self, sport_type: str) -> List[Dict[str, Any]]:
        """Get arbitrage opportunities for a specific sport"""
        return await self.db.execute_query(
            'arbitrage_opportunities', 'select',
            filters={
                'sport_type': sport_type,
                'is_active': True,
                'expires_at': {'gte': datetime.now().isoformat()}
            }
        )
    
    async def get_arbitrage_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get arbitrage opportunities history"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        return await self.db.execute_query(
            'arbitrage_opportunities', 'select',
            filters={
                'created_at': {'gte': cutoff_date.isoformat()}
            }
        )
    
    async def get_arbitrage_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get arbitrage statistics for the last N days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        opportunities = await self.db.execute_query(
            'arbitrage_opportunities', 'select',
            filters={
                'created_at': {'gte': cutoff_date.isoformat()}
            }
        )
        
        if not opportunities:
            return {
                'total_opportunities': 0,
                'avg_profit_percentage': 0,
                'best_profit_percentage': 0,
                'active_count': 0
            }
        
        # Calculate statistics
        profit_percentages = [float(opp['profit_percentage']) for opp in opportunities]
        active_count = len([opp for opp in opportunities if opp['is_active']])
        
        return {
            'total_opportunities': len(opportunities),
            'avg_profit_percentage': sum(profit_percentages) / len(profit_percentages),
            'best_profit_percentage': max(profit_percentages),
            'active_count': active_count
        }