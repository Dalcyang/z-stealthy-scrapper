from typing import Dict, Optional, Any
import structlog

logger = structlog.get_logger(__name__)


class ArbitrageCalculator:
    """Mathematical calculator for arbitrage opportunities"""
    
    def __init__(self):
        self.min_profit_threshold = 0.01  # 0.01% minimum profit to consider valid
    
    def calculate_two_way_arbitrage(self, 
                                  odds_1: float, 
                                  odds_2: float, 
                                  total_stake: float = 100.0) -> Optional[Dict[str, Any]]:
        """
        Calculate 2-way arbitrage (e.g., Over/Under, Home/Away)
        
        Args:
            odds_1: Decimal odds for selection 1
            odds_2: Decimal odds for selection 2
            total_stake: Total amount to stake across both selections
            
        Returns:
            Dict with arbitrage details or None if no arbitrage exists
        """
        try:
            if odds_1 <= 1.0 or odds_2 <= 1.0:
                return None
            
            # Calculate implied probabilities
            implied_prob_1 = 1 / odds_1
            implied_prob_2 = 1 / odds_2
            
            # Total implied probability
            total_implied_prob = implied_prob_1 + implied_prob_2
            
            # Check if arbitrage exists (total implied probability < 1)
            if total_implied_prob >= 1.0:
                return None
            
            # Calculate profit percentage
            profit_percentage = ((1 / total_implied_prob) - 1) * 100
            
            if profit_percentage < self.min_profit_threshold:
                return None
            
            # Calculate optimal stakes
            stake_1 = (total_stake * implied_prob_1) / total_implied_prob
            stake_2 = (total_stake * implied_prob_2) / total_implied_prob
            
            # Calculate returns
            return_1 = stake_1 * odds_1
            return_2 = stake_2 * odds_2
            
            # Expected profit (should be the same regardless of outcome)
            expected_profit = return_1 - total_stake
            
            return {
                'arbitrage_exists': True,
                'profit_percentage': round(profit_percentage, 4),
                'total_stake': round(total_stake, 2),
                'stake_1': round(stake_1, 2),
                'stake_2': round(stake_2, 2),
                'return_1': round(return_1, 2),
                'return_2': round(return_2, 2),
                'expected_profit': round(expected_profit, 2),
                'roi_percentage': round((expected_profit / total_stake) * 100, 4),
                'total_implied_probability': round(total_implied_prob, 6),
                'implied_prob_1': round(implied_prob_1, 6),
                'implied_prob_2': round(implied_prob_2, 6)
            }
            
        except (ZeroDivisionError, ValueError) as e:
            logger.debug("Error calculating two-way arbitrage", error=str(e))
            return None
    
    def calculate_three_way_arbitrage(self, 
                                    odds_1: float, 
                                    odds_2: float, 
                                    odds_3: float,
                                    total_stake: float = 100.0) -> Optional[Dict[str, Any]]:
        """
        Calculate 3-way arbitrage (e.g., Home/Draw/Away)
        
        Args:
            odds_1: Decimal odds for selection 1
            odds_2: Decimal odds for selection 2
            odds_3: Decimal odds for selection 3
            total_stake: Total amount to stake across all selections
            
        Returns:
            Dict with arbitrage details or None if no arbitrage exists
        """
        try:
            if any(odds <= 1.0 for odds in [odds_1, odds_2, odds_3]):
                return None
            
            # Calculate implied probabilities
            implied_prob_1 = 1 / odds_1
            implied_prob_2 = 1 / odds_2
            implied_prob_3 = 1 / odds_3
            
            # Total implied probability
            total_implied_prob = implied_prob_1 + implied_prob_2 + implied_prob_3
            
            # Check if arbitrage exists
            if total_implied_prob >= 1.0:
                return None
            
            # Calculate profit percentage
            profit_percentage = ((1 / total_implied_prob) - 1) * 100
            
            if profit_percentage < self.min_profit_threshold:
                return None
            
            # Calculate optimal stakes
            stake_1 = (total_stake * implied_prob_1) / total_implied_prob
            stake_2 = (total_stake * implied_prob_2) / total_implied_prob
            stake_3 = (total_stake * implied_prob_3) / total_implied_prob
            
            # Calculate returns
            return_1 = stake_1 * odds_1
            return_2 = stake_2 * odds_2
            return_3 = stake_3 * odds_3
            
            # Expected profit
            expected_profit = return_1 - total_stake
            
            return {
                'arbitrage_exists': True,
                'profit_percentage': round(profit_percentage, 4),
                'total_stake': round(total_stake, 2),
                'stake_1': round(stake_1, 2),
                'stake_2': round(stake_2, 2),
                'stake_3': round(stake_3, 2),
                'return_1': round(return_1, 2),
                'return_2': round(return_2, 2),
                'return_3': round(return_3, 2),
                'expected_profit': round(expected_profit, 2),
                'roi_percentage': round((expected_profit / total_stake) * 100, 4),
                'total_implied_probability': round(total_implied_prob, 6),
                'implied_prob_1': round(implied_prob_1, 6),
                'implied_prob_2': round(implied_prob_2, 6),
                'implied_prob_3': round(implied_prob_3, 6)
            }
            
        except (ZeroDivisionError, ValueError) as e:
            logger.debug("Error calculating three-way arbitrage", error=str(e))
            return None
    
    def calculate_implied_probability(self, odds: float) -> Optional[float]:
        """Calculate implied probability from decimal odds"""
        try:
            if odds <= 1.0:
                return None
            return 1 / odds
        except ZeroDivisionError:
            return None
    
    def calculate_bookmaker_margin(self, odds_list: list) -> Optional[float]:
        """
        Calculate bookmaker margin from a list of odds
        
        Args:
            odds_list: List of decimal odds for all possible outcomes
            
        Returns:
            Margin percentage or None if invalid
        """
        try:
            if not odds_list or any(odds <= 1.0 for odds in odds_list):
                return None
            
            total_implied_prob = sum(1 / odds for odds in odds_list)
            margin = (total_implied_prob - 1) * 100
            
            return round(margin, 4)
            
        except (ZeroDivisionError, ValueError):
            return None
    
    def calculate_kelly_criterion(self, 
                                odds: float, 
                                win_probability: float,
                                bankroll: float) -> Optional[float]:
        """
        Calculate optimal stake using Kelly Criterion
        
        Args:
            odds: Decimal odds
            win_probability: Estimated probability of winning (0-1)
            bankroll: Total available bankroll
            
        Returns:
            Optimal stake amount or None if not favorable
        """
        try:
            if odds <= 1.0 or win_probability <= 0 or win_probability >= 1:
                return None
            
            # Kelly formula: f = (bp - q) / b
            # where b = odds - 1, p = win_probability, q = 1 - win_probability
            b = odds - 1
            p = win_probability
            q = 1 - win_probability
            
            kelly_fraction = (b * p - q) / b
            
            # Only bet if Kelly fraction is positive
            if kelly_fraction <= 0:
                return None
            
            # Apply fractional Kelly (typically 25% of full Kelly) for risk management
            conservative_fraction = kelly_fraction * 0.25
            
            optimal_stake = bankroll * conservative_fraction
            
            return round(max(0, optimal_stake), 2)
            
        except (ZeroDivisionError, ValueError):
            return None
    
    def calculate_value_bet(self, 
                          odds: float, 
                          estimated_probability: float) -> Optional[Dict[str, Any]]:
        """
        Calculate if a bet has positive expected value
        
        Args:
            odds: Decimal odds offered by bookmaker
            estimated_probability: Your estimated probability of the outcome
            
        Returns:
            Dict with value bet analysis or None if no value
        """
        try:
            if odds <= 1.0 or estimated_probability <= 0 or estimated_probability >= 1:
                return None
            
            # Calculate expected value
            expected_value = (odds * estimated_probability) - 1
            
            if expected_value <= 0:
                return None
            
            # Calculate value percentage
            value_percentage = expected_value * 100
            
            # Calculate implied probability from odds
            implied_probability = 1 / odds
            
            return {
                'has_value': True,
                'expected_value': round(expected_value, 6),
                'value_percentage': round(value_percentage, 4),
                'estimated_probability': round(estimated_probability, 6),
                'implied_probability': round(implied_probability, 6),
                'probability_difference': round(estimated_probability - implied_probability, 6)
            }
            
        except (ZeroDivisionError, ValueError):
            return None
    
    def calculate_dutch_book(self, stakes_and_odds: list) -> Optional[Dict[str, Any]]:
        """
        Calculate Dutch book (guaranteed profit regardless of outcome)
        
        Args:
            stakes_and_odds: List of tuples [(stake1, odds1), (stake2, odds2), ...]
            
        Returns:
            Dict with Dutch book analysis
        """
        try:
            if not stakes_and_odds:
                return None
            
            total_stake = sum(stake for stake, _ in stakes_and_odds)
            
            if total_stake <= 0:
                return None
            
            # Calculate potential returns for each outcome
            returns = []
            for stake, odds in stakes_and_odds:
                if odds <= 1.0 or stake < 0:
                    return None
                returns.append(stake * odds)
            
            # Check if all returns are greater than total stake (guaranteed profit)
            min_return = min(returns)
            guaranteed_profit = min_return - total_stake
            
            if guaranteed_profit <= 0:
                return None
            
            profit_percentage = (guaranteed_profit / total_stake) * 100
            
            return {
                'guaranteed_profit': round(guaranteed_profit, 2),
                'profit_percentage': round(profit_percentage, 4),
                'total_stake': round(total_stake, 2),
                'min_return': round(min_return, 2),
                'all_returns': [round(ret, 2) for ret in returns]
            }
            
        except (ValueError, ZeroDivisionError):
            return None
    
    def optimize_stakes_for_equal_profit(self, odds_list: list, total_stake: float) -> Optional[Dict[str, Any]]:
        """
        Optimize stakes to ensure equal profit regardless of outcome
        
        Args:
            odds_list: List of decimal odds for each selection
            total_stake: Total amount to distribute across selections
            
        Returns:
            Dict with optimized stakes or None if impossible
        """
        try:
            if not odds_list or any(odds <= 1.0 for odds in odds_list) or total_stake <= 0:
                return None
            
            # Calculate implied probabilities
            implied_probs = [1 / odds for odds in odds_list]
            total_implied_prob = sum(implied_probs)
            
            # Check if arbitrage is possible
            if total_implied_prob >= 1.0:
                return None
            
            # Calculate optimal stakes for equal profit
            stakes = []
            for prob in implied_probs:
                stake = (total_stake * prob) / total_implied_prob
                stakes.append(round(stake, 2))
            
            # Calculate returns and profit
            returns = [stake * odds for stake, odds in zip(stakes, odds_list)]
            profit = returns[0] - total_stake  # Should be same for all outcomes
            
            return {
                'stakes': stakes,
                'returns': [round(ret, 2) for ret in returns],
                'guaranteed_profit': round(profit, 2),
                'profit_percentage': round((profit / total_stake) * 100, 4),
                'total_stake_used': round(sum(stakes), 2)
            }
            
        except (ValueError, ZeroDivisionError):
            return None