import os
import asyncio
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone
import structlog
from supabase import create_client, Client
from postgrest import APIError
from tenacity import retry, stop_after_attempt, wait_exponential

from ..models import Odds, SportEvent, Bookmaker, ArbitrageOpportunity

logger = structlog.get_logger(__name__)


class SupabaseManager:
    """Advanced Supabase client with connection management and error handling"""
    
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.service_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if not all([self.supabase_url, self.supabase_key]):
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
        self.service_client: Optional[Client] = None
        
        if self.service_key:
            self.service_client = create_client(self.supabase_url, self.service_key)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def execute_query(self, table: str, operation: str, data: Optional[Dict] = None, 
                          filters: Optional[Dict] = None, use_service: bool = False) -> Dict[str, Any]:
        """Execute database query with retry logic"""
        try:
            client = self.service_client if use_service and self.service_client else self.client
            query = client.table(table)
            
            if operation == 'insert':
                result = query.insert(data).execute()
            elif operation == 'update':
                query = self._apply_filters(query, filters)
                result = query.update(data).execute()
            elif operation == 'delete':
                query = self._apply_filters(query, filters)
                result = query.delete().execute()
            elif operation == 'select':
                query = self._apply_filters(query, filters)
                result = query.execute()
            else:
                raise ValueError(f"Unsupported operation: {operation}")
                
            return result.data
            
        except APIError as e:
            logger.error("Supabase API error", error=str(e), table=table, operation=operation)
            raise
        except Exception as e:
            logger.error("Database operation failed", error=str(e), table=table, operation=operation)
            raise

    def _apply_filters(self, query, filters: Optional[Dict]) -> Any:
        """Apply filters to query"""
        if not filters:
            return query
            
        for key, value in filters.items():
            if isinstance(value, dict):
                # Handle complex filters like {'gte': 1.5} or {'in': [1, 2, 3]}
                for operator, filter_value in value.items():
                    if operator == 'eq':
                        query = query.eq(key, filter_value)
                    elif operator == 'gte':
                        query = query.gte(key, filter_value)
                    elif operator == 'lte':
                        query = query.lte(key, filter_value)
                    elif operator == 'gt':
                        query = query.gt(key, filter_value)
                    elif operator == 'lt':
                        query = query.lt(key, filter_value)
                    elif operator == 'in':
                        query = query.in_(key, filter_value)
                    elif operator == 'like':
                        query = query.like(key, filter_value)
                    elif operator == 'ilike':
                        query = query.ilike(key, filter_value)
            else:
                # Simple equality filter
                query = query.eq(key, value)
        
        return query

    # Bookmaker operations
    async def create_bookmaker(self, bookmaker: Bookmaker) -> Dict[str, Any]:
        """Create a new bookmaker"""
        data = bookmaker.dict(exclude={'id', 'created_at', 'updated_at'})
        data['created_at'] = datetime.now(timezone.utc).isoformat()
        data['updated_at'] = datetime.now(timezone.utc).isoformat()
        
        result = await self.execute_query('bookmakers', 'insert', data)
        logger.info("Bookmaker created", bookmaker_name=bookmaker.name)
        return result[0] if result else {}

    async def get_bookmaker_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get bookmaker by name"""
        result = await self.execute_query('bookmakers', 'select', filters={'name': name})
        return result[0] if result else None

    async def get_all_bookmakers(self) -> List[Dict[str, Any]]:
        """Get all active bookmakers"""
        return await self.execute_query('bookmakers', 'select', filters={'is_active': True})

    # Sport Event operations
    async def create_sport_event(self, event: SportEvent) -> Dict[str, Any]:
        """Create a new sport event"""
        data = event.dict(exclude={'id', 'created_at', 'updated_at'})
        data['created_at'] = datetime.now(timezone.utc).isoformat()
        data['updated_at'] = datetime.now(timezone.utc).isoformat()
        data['event_date'] = event.event_date.isoformat()
        
        result = await self.execute_query('sport_events', 'insert', data)
        logger.info("Sport event created", home_team=event.home_team, away_team=event.away_team)
        return result[0] if result else {}

    async def get_event_by_teams_and_date(self, home_team: str, away_team: str, 
                                        event_date: datetime) -> Optional[Dict[str, Any]]:
        """Get event by team names and date"""
        filters = {
            'home_team': home_team,
            'away_team': away_team,
            'event_date': {'gte': event_date.date().isoformat()}
        }
        result = await self.execute_query('sport_events', 'select', filters=filters)
        return result[0] if result else None

    async def get_upcoming_events(self, sport_type: Optional[str] = None, 
                                limit: int = 100) -> List[Dict[str, Any]]:
        """Get upcoming sport events"""
        filters = {
            'event_date': {'gte': datetime.now(timezone.utc).isoformat()}
        }
        if sport_type:
            filters['sport_type'] = sport_type
            
        result = await self.execute_query('sport_events', 'select', filters=filters)
        return result[:limit] if result else []

    # Odds operations
    async def create_odds(self, odds: Odds) -> Dict[str, Any]:
        """Create new odds"""
        data = odds.dict(exclude={'id', 'created_at'})
        data['created_at'] = datetime.now(timezone.utc).isoformat()
        data['last_updated'] = odds.last_updated.isoformat()
        
        result = await self.execute_query('odds', 'insert', data)
        logger.info("Odds created", event_id=odds.event_id, bookmaker_id=odds.bookmaker_id)
        return result[0] if result else {}

    async def update_odds(self, odds_id: int, odds_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update existing odds"""
        odds_data['last_updated'] = datetime.now(timezone.utc).isoformat()
        result = await self.execute_query('odds', 'update', data=odds_data, filters={'id': odds_id})
        return result[0] if result else {}

    async def get_odds_by_event(self, event_id: int, bookmaker_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all odds for an event"""
        filters = {'event_id': event_id, 'is_available': True}
        if bookmaker_id:
            filters['bookmaker_id'] = bookmaker_id
            
        return await self.execute_query('odds', 'select', filters=filters)

    async def get_latest_odds(self, event_id: int, bet_type: str) -> List[Dict[str, Any]]:
        """Get latest odds for specific event and bet type"""
        filters = {
            'event_id': event_id,
            'bet_type': bet_type,
            'is_available': True
        }
        result = await self.execute_query('odds', 'select', filters=filters)
        
        # Group by bookmaker and return latest for each
        latest_odds = {}
        for odd in result:
            bookmaker_id = odd['bookmaker_id']
            if (bookmaker_id not in latest_odds or 
                odd['last_updated'] > latest_odds[bookmaker_id]['last_updated']):
                latest_odds[bookmaker_id] = odd
                
        return list(latest_odds.values())

    # Arbitrage operations
    async def create_arbitrage_opportunity(self, arbitrage: ArbitrageOpportunity) -> Dict[str, Any]:
        """Create new arbitrage opportunity"""
        data = arbitrage.dict(exclude={'id', 'created_at'})
        data['created_at'] = datetime.now(timezone.utc).isoformat()
        data['event_date'] = arbitrage.event_date.isoformat()
        
        if arbitrage.expires_at:
            data['expires_at'] = arbitrage.expires_at.isoformat()
            
        result = await self.execute_query('arbitrage_opportunities', 'insert', data)
        logger.info("Arbitrage opportunity created", 
                   profit_percentage=arbitrage.profit_percentage,
                   event_id=arbitrage.event_id)
        return result[0] if result else {}

    async def get_active_arbitrage_opportunities(self, min_profit: float = 1.0) -> List[Dict[str, Any]]:
        """Get active arbitrage opportunities above minimum profit threshold"""
        filters = {
            'is_active': True,
            'profit_percentage': {'gte': min_profit},
            'expires_at': {'gte': datetime.now(timezone.utc).isoformat()}
        }
        return await self.execute_query('arbitrage_opportunities', 'select', filters=filters)

    async def deactivate_expired_arbitrage(self) -> int:
        """Deactivate expired arbitrage opportunities"""
        data = {'is_active': False}
        filters = {
            'expires_at': {'lt': datetime.now(timezone.utc).isoformat()},
            'is_active': True
        }
        result = await self.execute_query('arbitrage_opportunities', 'update', 
                                        data=data, filters=filters)
        count = len(result) if result else 0
        logger.info("Deactivated expired arbitrage opportunities", count=count)
        return count

    # Bulk operations for performance
    async def bulk_insert_odds(self, odds_list: List[Odds]) -> List[Dict[str, Any]]:
        """Bulk insert odds for better performance"""
        data_list = []
        current_time = datetime.now(timezone.utc).isoformat()
        
        for odds in odds_list:
            data = odds.dict(exclude={'id', 'created_at'})
            data['created_at'] = current_time
            data['last_updated'] = odds.last_updated.isoformat()
            data_list.append(data)
        
        if not data_list:
            return []
            
        result = await self.execute_query('odds', 'insert', data=data_list)
        logger.info("Bulk inserted odds", count=len(data_list))
        return result

    async def cleanup_old_odds(self, days_old: int = 7) -> int:
        """Remove odds older than specified days"""
        cutoff_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_old)
        
        filters = {
            'last_updated': {'lt': cutoff_date.isoformat()}
        }
        result = await self.execute_query('odds', 'delete', filters=filters, use_service=True)
        count = len(result) if result else 0
        logger.info("Cleaned up old odds", count=count, days_old=days_old)
        return count