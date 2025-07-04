import asyncio
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import structlog
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from playwright.async_api import Page

from ..models import Odds, SportEvent, Bookmaker, BetType, SportType, BookmakerEnum
from ..utils import StealthScraper
from ..database import SupabaseManager

logger = structlog.get_logger(__name__)


class BaseScraper(ABC):
    """Base scraper class with common functionality for all bookmakers"""
    
    def __init__(self, 
                 bookmaker_name: BookmakerEnum,
                 base_url: str,
                 stealth_scraper: StealthScraper,
                 db_manager: SupabaseManager):
        
        self.bookmaker_name = bookmaker_name
        self.base_url = base_url
        self.stealth_scraper = stealth_scraper
        self.db_manager = db_manager
        
        # Scraper configuration
        self.max_retries = 3
        self.timeout = 30
        self.sport_urls = {}  # To be defined by each scraper
        self.bookmaker_id: Optional[int] = None
        
        # Odds extraction patterns (to be customized per bookmaker)
        self.odds_patterns = {
            'decimal': r'(\d+\.\d{2,3})',
            'fractional': r'(\d+/\d+)',
            'american': r'([+-]\d+)'
        }
        
        # Team name normalization patterns
        self.team_name_patterns = [
            (r'\s+vs?\s+', ' vs '),
            (r'\s+v\s+', ' vs '),
            (r'\s{2,}', ' '),
            (r'^\s+|\s+$', '')
        ]
        
    async def initialize(self):
        """Initialize scraper and get bookmaker ID"""
        bookmaker = await self.db_manager.get_bookmaker_by_name(self.bookmaker_name.value)
        
        if not bookmaker:
            # Create bookmaker if it doesn't exist
            new_bookmaker = Bookmaker(
                name=self.bookmaker_name,
                display_name=self.get_display_name(),
                website_url=self.base_url
            )
            bookmaker = await self.db_manager.create_bookmaker(new_bookmaker)
        
        self.bookmaker_id = bookmaker['id']
        logger.info("Scraper initialized", 
                   bookmaker=self.bookmaker_name.value,
                   bookmaker_id=self.bookmaker_id)

    @abstractmethod
    def get_display_name(self) -> str:
        """Get human-readable display name for the bookmaker"""
        pass

    @abstractmethod
    async def scrape_sports_odds(self, sport_types: List[SportType]) -> List[Odds]:
        """Scrape odds for specified sports"""
        pass

    @abstractmethod
    async def scrape_event_odds(self, event_url: str) -> List[Odds]:
        """Scrape odds for a specific event"""
        pass

    @abstractmethod
    def parse_odds_from_element(self, element: Any, bet_type: BetType) -> Optional[float]:
        """Parse odds value from DOM element"""
        pass

    async def scrape_all_odds(self) -> List[Odds]:
        """Scrape all available odds from the bookmaker"""
        try:
            all_odds = []
            
            # Get all supported sports
            sports_to_scrape = [
                SportType.FOOTBALL,
                SportType.RUGBY,
                SportType.CRICKET,
                SportType.TENNIS,
                SportType.BASKETBALL,
                SportType.SOCCER
            ]
            
            for sport in sports_to_scrape:
                try:
                    sport_odds = await self.scrape_sports_odds([sport])
                    all_odds.extend(sport_odds)
                    
                    # Add delay between sports
                    await self.stealth_scraper.random_delay(2, 5)
                    
                except Exception as e:
                    logger.error("Failed to scrape sport odds", 
                               sport=sport.value,
                               bookmaker=self.bookmaker_name.value,
                               error=str(e))
            
            logger.info("Completed odds scraping", 
                       bookmaker=self.bookmaker_name.value,
                       total_odds=len(all_odds))
            
            return all_odds
            
        except Exception as e:
            logger.error("Failed to scrape all odds", 
                        bookmaker=self.bookmaker_name.value,
                        error=str(e))
            return []

    def normalize_team_name(self, team_name: str) -> str:
        """Normalize team names for consistency"""
        normalized = team_name.strip()
        
        for pattern, replacement in self.team_name_patterns:
            normalized = re.sub(pattern, replacement, normalized)
        
        return normalized

    def extract_odds_value(self, odds_text: str, odds_format: str = 'decimal') -> Optional[float]:
        """Extract odds value from text"""
        if not odds_text:
            return None
        
        # Clean the text
        cleaned_text = re.sub(r'[^\d\.\-\+/]', '', odds_text.strip())
        
        try:
            if odds_format == 'decimal':
                match = re.search(self.odds_patterns['decimal'], cleaned_text)
                if match:
                    odds = float(match.group(1))
                    return odds if odds >= 1.0 else None
                    
            elif odds_format == 'fractional':
                match = re.search(self.odds_patterns['fractional'], cleaned_text)
                if match:
                    numerator, denominator = match.group(1).split('/')
                    return (float(numerator) / float(denominator)) + 1.0
                    
            elif odds_format == 'american':
                match = re.search(self.odds_patterns['american'], cleaned_text)
                if match:
                    american_odds = int(match.group(1))
                    if american_odds > 0:
                        return (american_odds / 100) + 1.0
                    else:
                        return (100 / abs(american_odds)) + 1.0
                        
        except (ValueError, ZeroDivisionError, AttributeError):
            pass
        
        return None

    async def create_or_get_event(self, 
                                home_team: str, 
                                away_team: str, 
                                sport_type: SportType,
                                event_date: datetime,
                                league: str = "Unknown") -> Optional[int]:
        """Create or get existing sport event"""
        
        # Normalize team names
        home_team = self.normalize_team_name(home_team)
        away_team = self.normalize_team_name(away_team)
        
        # Check if event already exists
        existing_event = await self.db_manager.get_event_by_teams_and_date(
            home_team, away_team, event_date
        )
        
        if existing_event:
            return existing_event['id']
        
        # Create new event
        new_event = SportEvent(
            sport_type=sport_type,
            home_team=home_team,
            away_team=away_team,
            event_date=event_date,
            league=league,
            country="South Africa"
        )
        
        try:
            created_event = await self.db_manager.create_sport_event(new_event)
            return created_event['id']
        except Exception as e:
            logger.error("Failed to create sport event", 
                        home_team=home_team,
                        away_team=away_team,
                        error=str(e))
            return None

    def parse_event_date(self, date_text: str) -> Optional[datetime]:
        """Parse event date from various text formats"""
        if not date_text:
            return None
        
        # Common date patterns used by SA bookmakers
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{1,2})-(\d{1,2})-(\d{4})',  # DD-MM-YYYY
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
            r'(\d{1,2})\s+(\w+)\s+(\d{4})',  # DD Month YYYY
        ]
        
        time_patterns = [
            r'(\d{1,2}):(\d{2})',  # HH:MM
            r'(\d{1,2}):(\d{2}):(\d{2})'  # HH:MM:SS
        ]
        
        try:
            # Try to parse with various patterns
            for pattern in date_patterns:
                match = re.search(pattern, date_text)
                if match:
                    if len(match.groups()) == 3:
                        if pattern.startswith(r'(\d{4})'):  # YYYY-MM-DD
                            year, month, day = match.groups()
                        else:  # DD/MM/YYYY or DD-MM-YYYY
                            day, month, year = match.groups()
                        
                        # Parse time if present
                        hour, minute = 0, 0
                        for time_pattern in time_patterns:
                            time_match = re.search(time_pattern, date_text)
                            if time_match:
                                hour = int(time_match.group(1))
                                minute = int(time_match.group(2))
                                break
                        
                        return datetime(int(year), int(month), int(day), hour, minute)
            
            # If no pattern matches, try relative dates like "Today", "Tomorrow"
            lower_text = date_text.lower()
            now = datetime.now()
            
            if 'today' in lower_text:
                return now.replace(hour=12, minute=0, second=0, microsecond=0)
            elif 'tomorrow' in lower_text:
                return (now + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            elif 'yesterday' in lower_text:
                return (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
                
        except (ValueError, AttributeError):
            pass
        
        # Default to current time if parsing fails
        logger.warning("Failed to parse event date", date_text=date_text)
        return datetime.now() + timedelta(hours=24)

    async def wait_for_page_load(self, driver_or_page: Union[Any, Page], timeout: int = 30):
        """Wait for page to fully load"""
        if hasattr(driver_or_page, 'wait_for_load_state'):
            # Playwright
            await driver_or_page.wait_for_load_state('networkidle', timeout=timeout * 1000)
        else:
            # Selenium
            WebDriverWait(driver_or_page, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

    def detect_bet_type(self, selection_text: str, market_text: str = "") -> BetType:
        """Detect bet type from selection and market text"""
        combined_text = f"{selection_text} {market_text}".lower()
        
        if any(keyword in combined_text for keyword in ['1x2', 'match result', 'full time', 'winner']):
            return BetType.MATCH_WINNER
        elif any(keyword in combined_text for keyword in ['over', 'under', 'total goals', 'total points']):
            return BetType.OVER_UNDER
        elif any(keyword in combined_text for keyword in ['handicap', 'spread', 'point spread']):
            return BetType.HANDICAP
        elif any(keyword in combined_text for keyword in ['both teams to score', 'btts', 'gg']):
            return BetType.BOTH_TEAMS_TO_SCORE
        elif any(keyword in combined_text for keyword in ['correct score', 'exact score']):
            return BetType.CORRECT_SCORE
        elif any(keyword in combined_text for keyword in ['first goal', 'anytime scorer']):
            return BetType.FIRST_GOAL_SCORER
        else:
            return BetType.MATCH_WINNER  # Default

    async def handle_cloudflare_challenge(self, driver_or_page: Union[Any, Page]) -> bool:
        """Handle Cloudflare challenges"""
        try:
            if hasattr(driver_or_page, 'title'):
                # Playwright
                title = await driver_or_page.title()
            else:
                # Selenium
                title = driver_or_page.title
            
            if 'cloudflare' in title.lower() or 'just a moment' in title.lower():
                logger.info("Cloudflare challenge detected, waiting...")
                
                # Wait for challenge to complete
                max_wait = 30
                for i in range(max_wait):
                    await asyncio.sleep(1)
                    
                    if hasattr(driver_or_page, 'title'):
                        current_title = await driver_or_page.title()
                    else:
                        current_title = driver_or_page.title
                    
                    if 'cloudflare' not in current_title.lower():
                        logger.info("Cloudflare challenge completed")
                        return True
                
                logger.warning("Cloudflare challenge timeout")
                return False
            
            return True
            
        except Exception as e:
            logger.error("Error handling Cloudflare challenge", error=str(e))
            return False

    def calculate_confidence_score(self, 
                                 odds_value: float, 
                                 page_load_time: float,
                                 element_visibility: bool) -> float:
        """Calculate confidence score for scraped odds"""
        score = 1.0
        
        # Penalize unusual odds values
        if odds_value < 1.01 or odds_value > 100:
            score -= 0.3
        
        # Penalize slow page loads
        if page_load_time > 10:
            score -= 0.2
        elif page_load_time > 5:
            score -= 0.1
        
        # Penalize if element wasn't clearly visible
        if not element_visibility:
            score -= 0.2
        
        return max(0.0, score)

    async def validate_scraped_data(self, odds_list: List[Odds]) -> List[Odds]:
        """Validate and filter scraped odds data"""
        valid_odds = []
        
        for odds in odds_list:
            # Basic validation
            if not (1.01 <= odds.odds_decimal <= 100.0):
                logger.warning("Invalid odds value", 
                             odds_decimal=odds.odds_decimal,
                             event_id=odds.event_id)
                continue
            
            # Check for reasonable confidence score
            if odds.confidence_score < 0.5:
                logger.warning("Low confidence odds", 
                             confidence_score=odds.confidence_score,
                             event_id=odds.event_id)
                # Still include but flag it
            
            valid_odds.append(odds)
        
        logger.info("Odds validation completed", 
                   total_scraped=len(odds_list),
                   valid_odds=len(valid_odds))
        
        return valid_odds