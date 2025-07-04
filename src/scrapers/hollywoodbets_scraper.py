import asyncio
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import structlog
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from playwright.async_api import Page

from .base_scraper import BaseScraper
from ..models import Odds, SportType, BetType, BookmakerEnum
from ..utils import StealthScraper
from ..database import SupabaseManager

logger = structlog.get_logger(__name__)


class HollywoodbetsScraper(BaseScraper):
    """Hollywoodbets-specific scraper implementation"""
    
    def __init__(self, stealth_scraper: StealthScraper, db_manager: SupabaseManager):
        super().__init__(
            bookmaker_name=BookmakerEnum.HOLLYWOODBETS,
            base_url="https://www.hollywoodbets.net",
            stealth_scraper=stealth_scraper,
            db_manager=db_manager
        )
        
        # Hollywoodbets-specific URL patterns
        self.sport_urls = {
            SportType.SOCCER: "/sports/soccer",
            SportType.RUGBY: "/sports/rugby",
            SportType.CRICKET: "/sports/cricket",
            SportType.TENNIS: "/sports/tennis",
            SportType.BASKETBALL: "/sports/basketball",
            SportType.FOOTBALL: "/sports/american-football"
        }
        
        # Hollywoodbets-specific selectors
        self.selectors = {
            'event_container': '.event-row, .match-row, .fixture',
            'team_names': '.team-name, .participant, .competitor',
            'odds_container': '.odds-container, .market-row, .price-row',
            'odds_value': '.odds, .price, .decimal-odds',
            'event_date': '.date, .time, .kick-off',
            'league_name': '.league, .competition, .tournament',
            'market_name': '.market-name, .bet-type, .market-title'
        }

    def get_display_name(self) -> str:
        return "Hollywoodbets"

    async def scrape_sports_odds(self, sport_types: List[SportType]) -> List[Odds]:
        """Scrape odds for specified sports from Hollywoodbets"""
        all_odds = []
        
        for sport_type in sport_types:
            try:
                logger.info("Scraping sport odds", sport=sport_type.value, bookmaker="Hollywoodbets")
                sport_odds = await self._scrape_sport_specific_odds(sport_type)
                all_odds.extend(sport_odds)
                
                # Add delay between sports
                await self.stealth_scraper.random_delay(3, 7)
                
            except Exception as e:
                logger.error("Failed to scrape sport", sport=sport_type.value, error=str(e))
        
        return await self.validate_scraped_data(all_odds)

    async def _scrape_sport_specific_odds(self, sport_type: SportType) -> List[Odds]:
        """Scrape odds for a specific sport"""
        sport_odds = []
        
        if sport_type not in self.sport_urls:
            logger.warning("Sport not supported", sport=sport_type.value)
            return sport_odds
        
        sport_url = f"{self.base_url}{self.sport_urls[sport_type]}"
        
        try:
            # Try Playwright first, fallback to HTTP requests
            try:
                sport_odds = await self._scrape_with_playwright(sport_url, sport_type)
            except Exception as e:
                logger.warning("Playwright scraping failed, trying HTTP", error=str(e))
                sport_odds = await self._scrape_with_http(sport_url, sport_type)
                
        except Exception as e:
            logger.error("All scraping methods failed", sport=sport_type.value, error=str(e))
        
        return sport_odds

    async def _scrape_with_playwright(self, url: str, sport_type: SportType) -> List[Odds]:
        """Scrape using Playwright for JavaScript-heavy content"""
        odds_list = []
        
        page = await self.stealth_scraper.get_playwright_page()
        start_time = time.time()
        
        try:
            # Navigate to the page
            await page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Handle potential Cloudflare challenges
            if not await self.handle_cloudflare_challenge(page):
                logger.warning("Failed to handle Cloudflare challenge")
                return odds_list
            
            # Simulate human behavior
            await self.stealth_scraper.simulate_human_behavior(page)
            
            # Wait for content to load
            await page.wait_for_selector(self.selectors['event_container'], timeout=15000)
            
            # Extract events
            events = await page.query_selector_all(self.selectors['event_container'])
            
            for event_element in events:
                try:
                    event_odds = await self._extract_event_odds_playwright(
                        event_element, page, sport_type, time.time() - start_time
                    )
                    odds_list.extend(event_odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract event odds", error=str(e))
            
        except Exception as e:
            logger.error("Playwright scraping error", url=url, error=str(e))
        
        finally:
            await page.close()
        
        return odds_list

    async def _extract_event_odds_playwright(self, 
                                           event_element, 
                                           page: Page, 
                                           sport_type: SportType,
                                           page_load_time: float) -> List[Odds]:
        """Extract odds from a single event using Playwright"""
        odds_list = []
        
        try:
            # Extract team names
            team_elements = await event_element.query_selector_all(self.selectors['team_names'])
            if len(team_elements) < 2:
                return odds_list
            
            home_team = await team_elements[0].text_content()
            away_team = await team_elements[1].text_content()
            
            if not home_team or not away_team:
                return odds_list
            
            # Extract event date
            date_element = await event_element.query_selector(self.selectors['event_date'])
            event_date = datetime.now() + timedelta(hours=24)  # Default
            
            if date_element:
                date_text = await date_element.text_content()
                parsed_date = self.parse_event_date(date_text)
                if parsed_date:
                    event_date = parsed_date
            
            # Extract league
            league_element = await event_element.query_selector(self.selectors['league_name'])
            league = "Unknown"
            if league_element:
                league_text = await league_element.text_content()
                if league_text:
                    league = league_text.strip()
            
            # Create or get event
            event_id = await self.create_or_get_event(
                home_team, away_team, sport_type, event_date, league
            )
            
            if not event_id:
                return odds_list
            
            # Extract odds from different markets
            odds_containers = await event_element.query_selector_all(self.selectors['odds_container'])
            
            for odds_container in odds_containers:
                try:
                    market_odds = await self._extract_market_odds_playwright(
                        odds_container, event_id, page_load_time
                    )
                    odds_list.extend(market_odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract market odds", error=str(e))
            
        except Exception as e:
            logger.debug("Failed to extract event odds", error=str(e))
        
        return odds_list

    async def _extract_market_odds_playwright(self, 
                                            market_element,
                                            event_id: int,
                                            page_load_time: float) -> List[Odds]:
        """Extract odds from a market using Playwright"""
        odds_list = []
        
        try:
            # Get market name
            market_name_element = await market_element.query_selector(self.selectors['market_name'])
            market_name = "Match Winner"  # Default
            
            if market_name_element:
                market_text = await market_name_element.text_content()
                if market_text:
                    market_name = market_text.strip()
            
            # Get all odds in this market
            odds_elements = await market_element.query_selector_all(self.selectors['odds_value'])
            
            for i, odds_element in enumerate(odds_elements):
                try:
                    odds_text = await odds_element.text_content()
                    if not odds_text:
                        continue
                    
                    odds_value = self.extract_odds_value(odds_text)
                    if not odds_value:
                        continue
                    
                    # Determine selection based on position
                    selections = ['Home', 'Draw', 'Away']
                    if i < len(selections):
                        selection = selections[i]
                    else:
                        selection = f"Selection_{i+1}"
                    
                    # Detect bet type
                    bet_type = self.detect_bet_type(selection, market_name)
                    
                    # Check if element is visible
                    is_visible = await odds_element.is_visible()
                    
                    # Calculate confidence score
                    confidence = self.calculate_confidence_score(
                        odds_value, page_load_time, is_visible
                    )
                    
                    # Create odds object
                    odds = Odds(
                        event_id=event_id,
                        bookmaker_id=self.bookmaker_id,
                        bet_type=bet_type,
                        selection=selection,
                        odds_decimal=odds_value,
                        last_updated=datetime.now(),
                        confidence_score=confidence,
                        original_data={
                            'market_name': market_name,
                            'odds_text': odds_text,
                            'page_load_time': page_load_time
                        }
                    )
                    
                    odds_list.append(odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract individual odds", error=str(e))
        
        except Exception as e:
            logger.debug("Failed to extract market odds", error=str(e))
        
        return odds_list

    async def _scrape_with_http(self, url: str, sport_type: SportType) -> List[Odds]:
        """Fallback scraping using HTTP requests and BeautifulSoup"""
        odds_list = []
        
        try:
            # Make HTTP request
            html_content = await self.stealth_scraper.make_request(
                url, use_cloudscraper=True
            )
            
            if not html_content:
                return odds_list
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find event containers
            event_containers = soup.select(self.selectors['event_container'])
            
            for event_container in event_containers:
                try:
                    event_odds = await self._extract_event_odds_soup(
                        event_container, sport_type
                    )
                    odds_list.extend(event_odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract event odds from soup", error=str(e))
            
        except Exception as e:
            logger.error("HTTP scraping error", url=url, error=str(e))
        
        return odds_list

    async def _extract_event_odds_soup(self, 
                                     event_container, 
                                     sport_type: SportType) -> List[Odds]:
        """Extract odds from event container using BeautifulSoup"""
        odds_list = []
        
        try:
            # Extract team names
            team_elements = event_container.select(self.selectors['team_names'])
            if len(team_elements) < 2:
                return odds_list
            
            home_team = team_elements[0].get_text(strip=True)
            away_team = team_elements[1].get_text(strip=True)
            
            if not home_team or not away_team:
                return odds_list
            
            # Extract event date
            date_element = event_container.select_one(self.selectors['event_date'])
            event_date = datetime.now() + timedelta(hours=24)  # Default
            
            if date_element:
                date_text = date_element.get_text(strip=True)
                parsed_date = self.parse_event_date(date_text)
                if parsed_date:
                    event_date = parsed_date
            
            # Extract league
            league_element = event_container.select_one(self.selectors['league_name'])
            league = "Unknown"
            if league_element:
                league = league_element.get_text(strip=True)
            
            # Create or get event
            event_id = await self.create_or_get_event(
                home_team, away_team, sport_type, event_date, league
            )
            
            if not event_id:
                return odds_list
            
            # Extract odds
            odds_containers = event_container.select(self.selectors['odds_container'])
            
            for odds_container in odds_containers:
                try:
                    market_odds = await self._extract_market_odds_soup(
                        odds_container, event_id
                    )
                    odds_list.extend(market_odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract market odds from soup", error=str(e))
            
        except Exception as e:
            logger.debug("Failed to extract event odds from soup", error=str(e))
        
        return odds_list

    async def _extract_market_odds_soup(self, 
                                      market_container,
                                      event_id: int) -> List[Odds]:
        """Extract odds from market container using BeautifulSoup"""
        odds_list = []
        
        try:
            # Get market name
            market_name_element = market_container.select_one(self.selectors['market_name'])
            market_name = "Match Winner"  # Default
            
            if market_name_element:
                market_name = market_name_element.get_text(strip=True)
            
            # Get odds elements
            odds_elements = market_container.select(self.selectors['odds_value'])
            
            for i, odds_element in enumerate(odds_elements):
                try:
                    odds_text = odds_element.get_text(strip=True)
                    if not odds_text:
                        continue
                    
                    odds_value = self.extract_odds_value(odds_text)
                    if not odds_value:
                        continue
                    
                    # Determine selection
                    selections = ['Home', 'Draw', 'Away']
                    if i < len(selections):
                        selection = selections[i]
                    else:
                        selection = f"Selection_{i+1}"
                    
                    # Detect bet type
                    bet_type = self.detect_bet_type(selection, market_name)
                    
                    # Create odds object
                    odds = Odds(
                        event_id=event_id,
                        bookmaker_id=self.bookmaker_id,
                        bet_type=bet_type,
                        selection=selection,
                        odds_decimal=odds_value,
                        last_updated=datetime.now(),
                        confidence_score=0.8,  # Default for HTTP scraping
                        original_data={
                            'market_name': market_name,
                            'odds_text': odds_text,
                            'scraping_method': 'http'
                        }
                    )
                    
                    odds_list.append(odds)
                    
                except Exception as e:
                    logger.debug("Failed to extract individual odds from soup", error=str(e))
        
        except Exception as e:
            logger.debug("Failed to extract market odds from soup", error=str(e))
        
        return odds_list

    async def scrape_event_odds(self, event_url: str) -> List[Odds]:
        """Scrape odds for a specific event URL"""
        try:
            # This would be implemented similar to sport scraping
            # but targeting a specific event page
            logger.info("Scraping specific event", url=event_url)
            
            # Implementation would depend on Hollywoodbets' specific event page structure
            # For now, return empty list
            return []
            
        except Exception as e:
            logger.error("Failed to scrape event odds", url=event_url, error=str(e))
            return []

    def parse_odds_from_element(self, element: Any, bet_type: BetType) -> Optional[float]:
        """Parse odds value from DOM element"""
        try:
            if hasattr(element, 'text_content'):
                # Playwright element
                odds_text = element.text_content()
            else:
                # BeautifulSoup element
                odds_text = element.get_text(strip=True)
            
            return self.extract_odds_value(odds_text)
            
        except Exception:
            return None