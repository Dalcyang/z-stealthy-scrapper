import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import structlog

from ..models import Odds, ArbitrageOpportunity, BetType, SportType
from ..database import SupabaseManager
from .arbitrage_calculator import ArbitrageCalculator

logger = structlog.get_logger(__name__)


class ArbitrageDetector:
    """Advanced arbitrage opportunity detector"""
    
    def __init__(self, 
                 db_manager: SupabaseManager,
                 min_profit_percentage: float = 1.0,
                 max_stake: float = 1000.0,
                 min_confidence: float = 0.7):
        
        self.db_manager = db_manager
        self.min_profit_percentage = min_profit_percentage
        self.max_stake = max_stake
        self.min_confidence = min_confidence
        
        self.calculator = ArbitrageCalculator()
        
        # Supported bet types for arbitrage
        self.supported_bet_types = [
            BetType.MATCH_WINNER,
            BetType.OVER_UNDER,
            BetType.HANDICAP
        ]
        
        # Minimum number of bookmakers required for arbitrage
        self.min_bookmakers = 2

    async def detect_all_arbitrage_opportunities(self) -> List[ArbitrageOpportunity]:
        """Detect all current arbitrage opportunities"""
        try:
            logger.info("Starting arbitrage detection")
            
            # Get all upcoming events
            upcoming_events = await self.db_manager.get_upcoming_events(limit=200)
            
            if not upcoming_events:
                logger.info("No upcoming events found")
                return []
            
            all_opportunities = []
            
            for event in upcoming_events:
                try:
                    event_opportunities = await self._detect_event_arbitrage(event)
                    all_opportunities.extend(event_opportunities)
                    
                except Exception as e:
                    logger.error("Failed to detect arbitrage for event", 
                               event_id=event['id'], error=str(e))
            
            # Filter and rank opportunities
            filtered_opportunities = await self._filter_and_rank_opportunities(all_opportunities)
            
            logger.info("Arbitrage detection completed", 
                       total_events=len(upcoming_events),
                       opportunities_found=len(filtered_opportunities))
            
            return filtered_opportunities
            
        except Exception as e:
            logger.error("Failed to detect arbitrage opportunities", error=str(e))
            return []

    async def _detect_event_arbitrage(self, event: Dict[str, Any]) -> List[ArbitrageOpportunity]:
        """Detect arbitrage opportunities for a specific event"""
        event_id = event['id']
        opportunities = []
        
        try:
            # Get all odds for this event
            event_odds = await self.db_manager.get_odds_by_event(event_id)
            
            if len(event_odds) < self.min_bookmakers:
                return opportunities
            
            # Group odds by bet type and selection
            grouped_odds = self._group_odds_by_market(event_odds)
            
            # Check each bet type for arbitrage opportunities
            for bet_type, market_odds in grouped_odds.items():
                if bet_type in self.supported_bet_types:
                    try:
                        market_opportunities = await self._detect_market_arbitrage(
                            event, bet_type, market_odds
                        )
                        opportunities.extend(market_opportunities)
                        
                    except Exception as e:
                        logger.debug("Failed to detect market arbitrage", 
                                   event_id=event_id, bet_type=bet_type, error=str(e))
            
        except Exception as e:
            logger.error("Failed to detect event arbitrage", 
                        event_id=event_id, error=str(e))
        
        return opportunities

    def _group_odds_by_market(self, odds_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Group odds by bet type and selection"""
        grouped = defaultdict(lambda: defaultdict(list))
        
        for odds in odds_list:
            bet_type = odds['bet_type']
            selection = odds['selection']
            grouped[bet_type][selection].append(odds)
        
        return grouped

    async def _detect_market_arbitrage(self, 
                                     event: Dict[str, Any],
                                     bet_type: str,
                                     market_odds: Dict[str, List[Dict[str, Any]]]) -> List[ArbitrageOpportunity]:
        """Detect arbitrage opportunities in a specific market"""
        opportunities = []
        
        try:
            # Get the best odds for each selection
            best_odds = {}
            
            for selection, odds_list in market_odds.items():
                if not odds_list:
                    continue
                
                # Find the best odds (highest value) for this selection
                best_odd = max(odds_list, key=lambda x: x['odds_decimal'])
                
                # Filter by confidence score
                if best_odd['confidence_score'] >= self.min_confidence:
                    best_odds[selection] = best_odd
            
            # Check if we have enough selections for arbitrage
            if len(best_odds) < 2:
                return opportunities
            
            # Check for 2-way arbitrage (e.g., Over/Under, Home/Away in handicap)
            if len(best_odds) == 2:
                opportunities.extend(await self._check_two_way_arbitrage(
                    event, bet_type, best_odds
                ))
            
            # Check for 3-way arbitrage (e.g., Home/Draw/Away)
            elif len(best_odds) == 3:
                opportunities.extend(await self._check_three_way_arbitrage(
                    event, bet_type, best_odds
                ))
            
        except Exception as e:
            logger.debug("Failed to detect market arbitrage", error=str(e))
        
        return opportunities

    async def _check_two_way_arbitrage(self, 
                                     event: Dict[str, Any],
                                     bet_type: str,
                                     best_odds: Dict[str, Dict[str, Any]]) -> List[ArbitrageOpportunity]:
        """Check for 2-way arbitrage opportunity"""
        opportunities = []
        
        try:
            selections = list(best_odds.keys())
            if len(selections) != 2:
                return opportunities
            
            odds_1 = best_odds[selections[0]]['odds_decimal']
            odds_2 = best_odds[selections[1]]['odds_decimal']
            
            # Calculate arbitrage
            arbitrage_result = self.calculator.calculate_two_way_arbitrage(
                odds_1, odds_2, self.max_stake
            )
            
            if arbitrage_result and arbitrage_result['profit_percentage'] >= self.min_profit_percentage:
                # Create arbitrage opportunity
                opportunity = await self._create_arbitrage_opportunity(
                    event, bet_type, best_odds, arbitrage_result
                )
                
                if opportunity:
                    opportunities.append(opportunity)
            
        except Exception as e:
            logger.debug("Failed to check two-way arbitrage", error=str(e))
        
        return opportunities

    async def _check_three_way_arbitrage(self, 
                                       event: Dict[str, Any],
                                       bet_type: str,
                                       best_odds: Dict[str, Dict[str, Any]]) -> List[ArbitrageOpportunity]:
        """Check for 3-way arbitrage opportunity"""
        opportunities = []
        
        try:
            selections = list(best_odds.keys())
            if len(selections) != 3:
                return opportunities
            
            odds_1 = best_odds[selections[0]]['odds_decimal']
            odds_2 = best_odds[selections[1]]['odds_decimal']
            odds_3 = best_odds[selections[2]]['odds_decimal']
            
            # Calculate arbitrage
            arbitrage_result = self.calculator.calculate_three_way_arbitrage(
                odds_1, odds_2, odds_3, self.max_stake
            )
            
            if arbitrage_result and arbitrage_result['profit_percentage'] >= self.min_profit_percentage:
                # Create arbitrage opportunity
                opportunity = await self._create_arbitrage_opportunity(
                    event, bet_type, best_odds, arbitrage_result
                )
                
                if opportunity:
                    opportunities.append(opportunity)
            
        except Exception as e:
            logger.debug("Failed to check three-way arbitrage", error=str(e))
        
        return opportunities

    async def _create_arbitrage_opportunity(self, 
                                          event: Dict[str, Any],
                                          bet_type: str,
                                          best_odds: Dict[str, Dict[str, Any]],
                                          arbitrage_result: Dict[str, Any]) -> Optional[ArbitrageOpportunity]:
        """Create arbitrage opportunity object"""
        try:
            # Prepare odds data for the opportunity
            odds_data = []
            selections = list(best_odds.keys())
            
            for i, selection in enumerate(selections):
                odds_info = best_odds[selection]
                stake_key = f'stake_{i+1}'
                
                odds_data.append({
                    'bookmaker_id': odds_info['bookmaker_id'],
                    'selection': selection,
                    'odds_decimal': odds_info['odds_decimal'],
                    'stake': arbitrage_result.get(stake_key, 0),
                    'potential_return': arbitrage_result.get(stake_key, 0) * odds_info['odds_decimal'],
                    'odds_id': odds_info['id']
                })
            
            # Calculate minimum confidence score
            min_confidence = min(odds['confidence_score'] for odds in best_odds.values())
            
            # Determine risk level
            risk_level = self._assess_risk_level(arbitrage_result['profit_percentage'], min_confidence)
            
            # Calculate expiration time (default to 1 hour from now)
            expires_at = datetime.now() + timedelta(hours=1)
            
            # Create opportunity
            opportunity = ArbitrageOpportunity(
                event_id=event['id'],
                sport_type=event['sport_type'],
                home_team=event['home_team'],
                away_team=event['away_team'],
                event_date=datetime.fromisoformat(event['event_date'].replace('Z', '+00:00')),
                bet_type=bet_type,
                profit_percentage=arbitrage_result['profit_percentage'],
                total_stake=arbitrage_result['total_stake'],
                expected_profit=arbitrage_result['expected_profit'],
                odds_data=odds_data,
                expires_at=expires_at,
                confidence_score=min_confidence,
                risk_level=risk_level
            )
            
            return opportunity
            
        except Exception as e:
            logger.error("Failed to create arbitrage opportunity", error=str(e))
            return None

    def _assess_risk_level(self, profit_percentage: float, confidence_score: float) -> str:
        """Assess risk level of arbitrage opportunity"""
        if profit_percentage >= 5.0 and confidence_score >= 0.9:
            return "low"
        elif profit_percentage >= 2.0 and confidence_score >= 0.8:
            return "medium"
        else:
            return "high"

    async def _filter_and_rank_opportunities(self, 
                                           opportunities: List[ArbitrageOpportunity]) -> List[ArbitrageOpportunity]:
        """Filter and rank arbitrage opportunities"""
        try:
            # Filter by minimum profit
            filtered = [
                opp for opp in opportunities 
                if opp.profit_percentage >= self.min_profit_percentage
            ]
            
            # Remove duplicates (same event and bet type)
            unique_opportunities = {}
            for opp in filtered:
                key = f"{opp.event_id}_{opp.bet_type}"
                
                if (key not in unique_opportunities or 
                    opp.profit_percentage > unique_opportunities[key].profit_percentage):
                    unique_opportunities[key] = opp
            
            # Convert back to list and sort by profit percentage
            final_opportunities = list(unique_opportunities.values())
            final_opportunities.sort(key=lambda x: x.profit_percentage, reverse=True)
            
            logger.info("Opportunities filtered and ranked", 
                       initial_count=len(opportunities),
                       final_count=len(final_opportunities))
            
            return final_opportunities
            
        except Exception as e:
            logger.error("Failed to filter and rank opportunities", error=str(e))
            return opportunities

    async def save_opportunities(self, opportunities: List[ArbitrageOpportunity]) -> int:
        """Save arbitrage opportunities to database"""
        saved_count = 0
        
        for opportunity in opportunities:
            try:
                await self.db_manager.create_arbitrage_opportunity(opportunity)
                saved_count += 1
                
                logger.info("Arbitrage opportunity saved", 
                           event_id=opportunity.event_id,
                           profit_percentage=opportunity.profit_percentage)
                
            except Exception as e:
                logger.error("Failed to save arbitrage opportunity", 
                           event_id=opportunity.event_id, error=str(e))
        
        return saved_count

    async def cleanup_expired_opportunities(self) -> int:
        """Remove expired arbitrage opportunities"""
        try:
            return await self.db_manager.deactivate_expired_arbitrage()
        except Exception as e:
            logger.error("Failed to cleanup expired opportunities", error=str(e))
            return 0

    async def get_best_opportunities(self, 
                                   limit: int = 10, 
                                   min_profit: float = None) -> List[Dict[str, Any]]:
        """Get best current arbitrage opportunities"""
        try:
            min_profit = min_profit or self.min_profit_percentage
            opportunities = await self.db_manager.get_active_arbitrage_opportunities(min_profit)
            
            # Sort by profit percentage
            sorted_opportunities = sorted(
                opportunities, 
                key=lambda x: x['profit_percentage'], 
                reverse=True
            )
            
            return sorted_opportunities[:limit]
            
        except Exception as e:
            logger.error("Failed to get best opportunities", error=str(e))
            return []