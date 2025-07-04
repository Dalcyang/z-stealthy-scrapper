import os
import asyncio
import signal
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import structlog
from dotenv import load_dotenv

from .models import SportType, BookmakerEnum
from .utils import StealthScraper, setup_logging, get_performance_logger, get_error_tracker
from .database import SupabaseManager
from .scrapers import HollywoodbetsScraper, BetwayScraper
from .arbitrage import ArbitrageDetector

# Load environment variables
load_dotenv()

logger = structlog.get_logger(__name__)


class OddsScrapingOrchestrator:
    """Main orchestrator for the sports betting odds scraping system"""
    
    def __init__(self):
        # Initialize components
        self.db_manager = SupabaseManager()
        self.stealth_scraper = StealthScraper(
            headless=os.getenv('HEADLESS_MODE', 'true').lower() == 'true',
            proxy_enabled=os.getenv('PROXY_ENABLED', 'false').lower() == 'true',
            rate_limit=float(os.getenv('SCRAPER_DELAY_MIN', '2')),
            max_concurrent=int(os.getenv('MAX_CONCURRENT_REQUESTS', '3'))
        )
        
        self.arbitrage_detector = ArbitrageDetector(
            db_manager=self.db_manager,
            min_profit_percentage=float(os.getenv('MIN_ARBITRAGE_PERCENTAGE', '1.0')),
            max_stake=float(os.getenv('MAX_STAKE_AMOUNT', '1000'))
        )
        
        # Performance tracking
        self.performance_logger = get_performance_logger()
        self.error_tracker = get_error_tracker()
        
        # Scrapers
        self.scrapers = {}
        self.active_scrapers = []
        
        # Runtime control
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        
        # Statistics
        self.session_stats = {
            'start_time': None,
            'total_odds_scraped': 0,
            'total_events_found': 0,
            'total_arbitrage_opportunities': 0,
            'errors_count': 0,
            'scrapers_completed': 0
        }

    async def initialize(self):
        """Initialize the orchestrator and all components"""
        logger.info("Initializing odds scraping orchestrator")
        
        try:
            # Initialize core components
            await self.stealth_scraper.initialize()
            
            # Initialize scrapers
            await self._initialize_scrapers()
            
            logger.info("Orchestrator initialization completed", 
                       scrapers_count=len(self.scrapers))
            
        except Exception as e:
            logger.error("Failed to initialize orchestrator", error=str(e))
            raise

    async def _initialize_scrapers(self):
        """Initialize all available scrapers"""
        scraper_classes = {
            BookmakerEnum.HOLLYWOODBETS: HollywoodbetsScraper,
            BookmakerEnum.BETWAY: BetwayScraper,
        }
        
        for bookmaker, scraper_class in scraper_classes.items():
            try:
                scraper = scraper_class(self.stealth_scraper, self.db_manager)
                await scraper.initialize()
                self.scrapers[bookmaker] = scraper
                self.active_scrapers.append(bookmaker)
                
                logger.info("Scraper initialized", bookmaker=bookmaker.value)
                
            except Exception as e:
                logger.error("Failed to initialize scraper", 
                           bookmaker=bookmaker.value, error=str(e))
                self.error_tracker.track_error(
                    "initialization_error", str(e), "scraper", bookmaker.value
                )

    async def run_full_scraping_cycle(self) -> Dict[str, Any]:
        """Run a complete scraping cycle across all active scrapers"""
        
        self.session_stats['start_time'] = datetime.now()
        self.performance_logger.start_timer("full_scraping_cycle")
        
        logger.info("Starting full scraping cycle", 
                   active_scrapers=len(self.active_scrapers))
        
        try:
            # Phase 1: Scrape odds from all bookmakers
            odds_results = await self._scrape_all_bookmakers()
            
            # Phase 2: Detect arbitrage opportunities
            arbitrage_results = await self._detect_arbitrage_opportunities()
            
            # Phase 3: Cleanup old data
            cleanup_results = await self._cleanup_old_data()
            
            # Compile results
            cycle_results = {
                'success': True,
                'duration_seconds': (datetime.now() - self.session_stats['start_time']).total_seconds(),
                'odds_results': odds_results,
                'arbitrage_results': arbitrage_results,
                'cleanup_results': cleanup_results,
                'session_stats': self.session_stats.copy()
            }
            
            self.performance_logger.end_timer("full_scraping_cycle", success=True, **cycle_results)
            
            logger.info("Full scraping cycle completed successfully", **cycle_results)
            
            return cycle_results
            
        except Exception as e:
            logger.error("Full scraping cycle failed", error=str(e))
            self.error_tracker.track_error("cycle_error", str(e), "orchestrator")
            
            self.performance_logger.end_timer("full_scraping_cycle", success=False)
            
            return {
                'success': False,
                'error': str(e),
                'session_stats': self.session_stats.copy()
            }

    async def _scrape_all_bookmakers(self) -> Dict[str, Any]:
        """Scrape odds from all active bookmakers"""
        
        logger.info("Starting bookmaker scraping phase")
        scraping_tasks = []
        
        # Create scraping tasks for each active scraper
        for bookmaker in self.active_scrapers:
            if bookmaker in self.scrapers:
                task = self._scrape_single_bookmaker(bookmaker)
                scraping_tasks.append(task)
        
        # Execute scraping tasks
        if scraping_tasks:
            scraping_results = await asyncio.gather(*scraping_tasks, return_exceptions=True)
        else:
            scraping_results = []
        
        # Process results
        successful_scrapes = 0
        total_odds = 0
        total_events = 0
        errors = []
        
        for result in scraping_results:
            if isinstance(result, Exception):
                errors.append(str(result))
                logger.error("Scraper task failed", error=str(result))
            elif isinstance(result, dict):
                if result.get('success'):
                    successful_scrapes += 1
                    total_odds += result.get('odds_count', 0)
                    total_events += result.get('events_count', 0)
                else:
                    errors.append(result.get('error', 'Unknown error'))
        
        # Update session stats
        self.session_stats['total_odds_scraped'] += total_odds
        self.session_stats['total_events_found'] += total_events
        self.session_stats['errors_count'] += len(errors)
        self.session_stats['scrapers_completed'] = successful_scrapes
        
        return {
            'successful_scrapes': successful_scrapes,
            'total_scrapers': len(self.active_scrapers),
            'total_odds': total_odds,
            'total_events': total_events,
            'errors': errors
        }

    async def _scrape_single_bookmaker(self, bookmaker: BookmakerEnum) -> Dict[str, Any]:
        """Scrape odds from a single bookmaker"""
        
        scraper = self.scrapers[bookmaker]
        start_time = time.time()
        
        try:
            logger.info("Starting scraper", bookmaker=bookmaker.value)
            
            # Scrape odds
            all_odds = await scraper.scrape_all_odds()
            
            # Store odds in database
            if all_odds:
                stored_odds = await self.db_manager.bulk_insert_odds(all_odds)
                stored_count = len(stored_odds)
            else:
                stored_count = 0
            
            duration = time.time() - start_time
            
            logger.info("Scraper completed successfully", 
                       bookmaker=bookmaker.value,
                       odds_extracted=len(all_odds),
                       odds_stored=stored_count,
                       duration=duration)
            
            return {
                'success': True,
                'bookmaker': bookmaker.value,
                'odds_count': len(all_odds),
                'events_count': len(set(odds.event_id for odds in all_odds)),
                'duration': duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            
            logger.error("Scraper failed", 
                        bookmaker=bookmaker.value, 
                        error=str(e),
                        duration=duration)
            
            self.error_tracker.track_error(
                "scraping_error", str(e), "scraper", bookmaker.value
            )
            
            return {
                'success': False,
                'bookmaker': bookmaker.value,
                'error': str(e),
                'duration': duration
            }

    async def _detect_arbitrage_opportunities(self) -> Dict[str, Any]:
        """Detect and save arbitrage opportunities"""
        
        logger.info("Starting arbitrage detection phase")
        start_time = time.time()
        
        try:
            # Detect opportunities
            opportunities = await self.arbitrage_detector.detect_all_arbitrage_opportunities()
            
            # Save opportunities to database
            saved_count = 0
            if opportunities:
                saved_count = await self.arbitrage_detector.save_opportunities(opportunities)
            
            # Clean up expired opportunities
            expired_count = await self.arbitrage_detector.cleanup_expired_opportunities()
            
            duration = time.time() - start_time
            
            # Update session stats
            self.session_stats['total_arbitrage_opportunities'] += len(opportunities)
            
            logger.info("Arbitrage detection completed", 
                       opportunities_found=len(opportunities),
                       opportunities_saved=saved_count,
                       expired_cleaned=expired_count,
                       duration=duration)
            
            return {
                'opportunities_found': len(opportunities),
                'opportunities_saved': saved_count,
                'expired_cleaned': expired_count,
                'duration': duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            
            logger.error("Arbitrage detection failed", error=str(e), duration=duration)
            self.error_tracker.track_error("arbitrage_error", str(e), "arbitrage_detector")
            
            return {
                'success': False,
                'error': str(e),
                'duration': duration
            }

    async def _cleanup_old_data(self) -> Dict[str, Any]:
        """Clean up old odds and expired data"""
        
        logger.info("Starting data cleanup phase")
        
        try:
            # Clean up old odds (older than 7 days)
            odds_cleaned = await self.db_manager.cleanup_old_odds(days_old=7)
            
            # Clean up expired arbitrage opportunities
            arbitrage_cleaned = await self.arbitrage_detector.cleanup_expired_opportunities()
            
            logger.info("Data cleanup completed", 
                       odds_cleaned=odds_cleaned,
                       arbitrage_cleaned=arbitrage_cleaned)
            
            return {
                'odds_cleaned': odds_cleaned,
                'arbitrage_cleaned': arbitrage_cleaned
            }
            
        except Exception as e:
            logger.error("Data cleanup failed", error=str(e))
            return {
                'success': False,
                'error': str(e)
            }

    async def run_continuous(self, interval_minutes: int = 30):
        """Run the scraper continuously at specified intervals"""
        
        self.is_running = True
        logger.info("Starting continuous scraping", interval_minutes=interval_minutes)
        
        try:
            while self.is_running and not self.shutdown_event.is_set():
                try:
                    # Run scraping cycle
                    cycle_results = await self.run_full_scraping_cycle()
                    
                    if cycle_results.get('success'):
                        logger.info("Scraping cycle completed successfully")
                    else:
                        logger.error("Scraping cycle failed", error=cycle_results.get('error'))
                    
                    # Wait for next cycle
                    logger.info(f"Waiting {interval_minutes} minutes for next cycle")
                    
                    try:
                        await asyncio.wait_for(
                            self.shutdown_event.wait(), 
                            timeout=interval_minutes * 60
                        )
                        break  # Shutdown event was set
                    except asyncio.TimeoutError:
                        continue  # Normal timeout, continue to next cycle
                    
                except Exception as e:
                    logger.error("Error in continuous scraping loop", error=str(e))
                    self.error_tracker.track_error("continuous_error", str(e), "orchestrator")
                    
                    # Wait a bit before retrying
                    await asyncio.sleep(60)
                    
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down gracefully")
        finally:
            await self.shutdown()

    async def get_live_arbitrage_opportunities(self, min_profit: float = 1.0, limit: int = 10):
        """Get current live arbitrage opportunities"""
        try:
            return await self.arbitrage_detector.get_best_opportunities(limit, min_profit)
        except Exception as e:
            logger.error("Failed to get live arbitrage opportunities", error=str(e))
            return []

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.shutdown_event.set()
            self.is_running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def shutdown(self):
        """Gracefully shutdown the orchestrator"""
        logger.info("Shutting down orchestrator")
        
        self.is_running = False
        
        try:
            # Cleanup stealth scraper
            await self.stealth_scraper.cleanup()
            
            logger.info("Orchestrator shutdown completed")
            
        except Exception as e:
            logger.error("Error during shutdown", error=str(e))


async def main():
    """Main entry point for the application"""
    
    # Setup logging
    setup_logging(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        log_file=os.getenv('LOG_FILE'),
        json_format=os.getenv('LOG_JSON', 'false').lower() == 'true'
    )
    
    logger.info("Starting Sports Betting Odds Scraper")
    
    # Create and initialize orchestrator
    orchestrator = OddsScrapingOrchestrator()
    orchestrator.setup_signal_handlers()
    
    try:
        await orchestrator.initialize()
        
        # Get run mode from environment
        run_mode = os.getenv('RUN_MODE', 'single')
        
        if run_mode == 'continuous':
            interval = int(os.getenv('SCRAPING_INTERVAL_MINUTES', '30'))
            await orchestrator.run_continuous(interval)
        else:
            # Single run mode
            result = await orchestrator.run_full_scraping_cycle()
            
            if result.get('success'):
                logger.info("Single scraping run completed successfully")
                
                # Show arbitrage opportunities if found
                opportunities = await orchestrator.get_live_arbitrage_opportunities()
                if opportunities:
                    logger.info("Found arbitrage opportunities", count=len(opportunities))
                    for opp in opportunities[:5]:  # Show top 5
                        logger.info("Arbitrage opportunity", 
                                   profit_percentage=opp['profit_percentage'],
                                   home_team=opp['home_team'],
                                   away_team=opp['away_team'])
                else:
                    logger.info("No arbitrage opportunities found")
            else:
                logger.error("Single scraping run failed", error=result.get('error'))
                
    except Exception as e:
        logger.error("Application failed", error=str(e))
        raise
    finally:
        await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())