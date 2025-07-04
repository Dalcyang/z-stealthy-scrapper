import os
import random
import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import aiohttp
import cloudscraper
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium_stealth import stealth
import undetected_chromedriver as uc
from playwright.async_api import async_playwright, Page, BrowserContext
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
from asyncio_throttle import Throttler

from .rate_limiter import RateLimiter
from .proxy_manager import ProxyManager

logger = structlog.get_logger(__name__)


class StealthScraper:
    """Advanced stealth scraper with comprehensive anti-detection features"""
    
    def __init__(self, 
                 headless: bool = True,
                 proxy_enabled: bool = False,
                 rate_limit: float = 2.0,
                 max_concurrent: int = 3,
                 user_data_dir: Optional[str] = None):
        
        self.headless = headless
        self.proxy_enabled = proxy_enabled
        self.rate_limiter = RateLimiter(rate_limit)
        self.throttler = Throttler(rate_limit=max_concurrent)
        self.proxy_manager = ProxyManager() if proxy_enabled else None
        self.user_agent = UserAgent()
        self.user_data_dir = user_data_dir
        
        # Browser instances
        self.selenium_driver: Optional[webdriver.Chrome] = None
        self.playwright_browser = None
        self.playwright_context: Optional[BrowserContext] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.cloudscraper_session = None
        
        # Anti-detection settings
        self.viewport_sizes = [
            (1920, 1080), (1366, 768), (1440, 900), (1536, 864),
            (1280, 720), (1600, 900), (1024, 768), (1280, 1024)
        ]
        
        self.mouse_patterns = [
            "linear", "bezier", "random_walk", "human_like"
        ]
        
        self.typing_speeds = {
            "slow": (0.1, 0.3),
            "normal": (0.05, 0.15),
            "fast": (0.02, 0.08)
        }

    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()

    async def initialize(self):
        """Initialize scraper components"""
        logger.info("Initializing stealth scraper")
        
        # Initialize rate limiter
        await self.rate_limiter.initialize()
        
        # Initialize proxy manager if enabled
        if self.proxy_manager:
            await self.proxy_manager.initialize()
        
        # Initialize CloudScraper session
        self.cloudscraper_session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )

    async def cleanup(self):
        """Cleanup all resources"""
        logger.info("Cleaning up stealth scraper")
        
        if self.selenium_driver:
            self.selenium_driver.quit()
            
        if self.playwright_context:
            await self.playwright_context.close()
            
        if self.playwright_browser:
            await self.playwright_browser.close()
            
        if self.session:
            await self.session.close()

    def get_random_headers(self) -> Dict[str, str]:
        """Generate randomized headers"""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': random.choice([
                'en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-ZA,en;q=0.9'
            ]),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

    def get_chrome_options(self, proxy: Optional[str] = None) -> Options:
        """Generate Chrome options with stealth settings"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless')
        
        # Basic stealth arguments
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Additional anti-detection
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        
        # Random viewport
        viewport = random.choice(self.viewport_sizes)
        options.add_argument(f'--window-size={viewport[0]},{viewport[1]}')
        
        # User agent
        options.add_argument(f'--user-agent={self.user_agent.random}')
        
        # Proxy
        if proxy:
            options.add_argument(f'--proxy-server={proxy}')
        
        # User data directory for session persistence
        if self.user_data_dir:
            options.add_argument(f'--user-data-dir={self.user_data_dir}')
        
        return options

    async def get_selenium_driver(self) -> webdriver.Chrome:
        """Get configured Selenium WebDriver"""
        if self.selenium_driver:
            return self.selenium_driver
        
        proxy = None
        if self.proxy_manager:
            proxy = await self.proxy_manager.get_proxy()
        
        try:
            # Use undetected-chromedriver for better stealth
            options = self.get_chrome_options(proxy)
            self.selenium_driver = uc.Chrome(options=options)
            
            # Apply stealth patches
            stealth(self.selenium_driver,
                   languages=["en-US", "en"],
                   vendor="Google Inc.",
                   platform="Win32",
                   webgl_vendor="Intel Inc.",
                   renderer="Intel Iris OpenGL Engine",
                   fix_hairline=True)
            
            # Execute additional stealth scripts
            self.selenium_driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
            """)
            
            logger.info("Selenium driver initialized successfully")
            return self.selenium_driver
            
        except Exception as e:
            logger.error("Failed to initialize Selenium driver", error=str(e))
            raise

    async def get_playwright_page(self) -> Page:
        """Get configured Playwright page"""
        if not self.playwright_browser:
            playwright = await async_playwright().start()
            
            proxy = None
            if self.proxy_manager:
                proxy_url = await self.proxy_manager.get_proxy()
                if proxy_url:
                    proxy = {"server": proxy_url}
            
            # Launch browser with stealth settings
            self.playwright_browser = await playwright.chromium.launch(
                headless=self.headless,
                proxy=proxy,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            
            # Create context with random viewport and user agent
            viewport = random.choice(self.viewport_sizes)
            self.playwright_context = await self.playwright_browser.new_context(
                viewport={'width': viewport[0], 'height': viewport[1]},
                user_agent=self.user_agent.random,
                extra_http_headers=self.get_random_headers()
            )
            
            # Add stealth scripts
            await self.playwright_context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock chrome runtime
                window.chrome = {
                    runtime: {}
                };
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
            """)
        
        page = await self.playwright_context.new_page()
        logger.info("Playwright page created successfully")
        return page

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        """Get configured aiohttp session"""
        if self.session:
            return self.session
        
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
            ssl=False
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.get_random_headers()
        )
        
        logger.info("aiohttp session created successfully")
        return self.session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def make_request(self, 
                          url: str, 
                          method: str = 'GET',
                          headers: Optional[Dict[str, str]] = None,
                          data: Optional[Dict] = None,
                          use_cloudscraper: bool = False) -> Optional[str]:
        """Make HTTP request with retry logic and rate limiting"""
        
        async with self.throttler:
            await self.rate_limiter.acquire()
            
            try:
                if use_cloudscraper:
                    # Use CloudScraper for cloudflare protection
                    if method.upper() == 'GET':
                        response = self.cloudscraper_session.get(url, headers=headers)
                    else:
                        response = self.cloudscraper_session.post(url, headers=headers, data=data)
                    
                    if response.status_code == 200:
                        logger.info("CloudScraper request successful", url=url)
                        return response.text
                    else:
                        logger.warning("CloudScraper request failed", 
                                     url=url, status_code=response.status_code)
                        return None
                else:
                    # Use aiohttp
                    session = await self.get_aiohttp_session()
                    
                    proxy = None
                    if self.proxy_manager:
                        proxy = await self.proxy_manager.get_proxy()
                    
                    request_headers = headers or self.get_random_headers()
                    
                    async with session.request(
                        method=method.upper(),
                        url=url,
                        headers=request_headers,
                        data=data,
                        proxy=proxy
                    ) as response:
                        if response.status == 200:
                            content = await response.text()
                            logger.info("HTTP request successful", url=url)
                            return content
                        else:
                            logger.warning("HTTP request failed", 
                                         url=url, status_code=response.status)
                            return None
                            
            except Exception as e:
                logger.error("Request failed", url=url, error=str(e))
                
                # Rotate proxy on failure if enabled
                if self.proxy_manager:
                    await self.proxy_manager.mark_proxy_failed()
                
                raise

    async def simulate_human_behavior(self, driver_or_page: Union[webdriver.Chrome, Page]):
        """Simulate human-like behavior"""
        
        # Random delay
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        if isinstance(driver_or_page, webdriver.Chrome):
            # Selenium human simulation
            await self._simulate_selenium_human_behavior(driver_or_page)
        else:
            # Playwright human simulation
            await self._simulate_playwright_human_behavior(driver_or_page)

    async def _simulate_selenium_human_behavior(self, driver: webdriver.Chrome):
        """Simulate human behavior with Selenium"""
        try:
            # Random mouse movements
            actions = ActionChains(driver)
            
            # Get viewport size
            viewport = driver.get_window_size()
            width, height = viewport['width'], viewport['height']
            
            # Random mouse movements
            for _ in range(random.randint(1, 3)):
                x = random.randint(0, width)
                y = random.randint(0, height)
                actions.move_by_offset(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.5))
            
            actions.perform()
            
            # Random scroll
            if random.choice([True, False]):
                scroll_amount = random.randint(100, 500)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
        except Exception as e:
            logger.debug("Human behavior simulation failed", error=str(e))

    async def _simulate_playwright_human_behavior(self, page: Page):
        """Simulate human behavior with Playwright"""
        try:
            # Random mouse movements
            for _ in range(random.randint(1, 3)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.5))
            
            # Random scroll
            if random.choice([True, False]):
                scroll_amount = random.randint(100, 500)
                await page.evaluate(f"window.scrollBy(0, {scroll_amount});")
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
        except Exception as e:
            logger.debug("Human behavior simulation failed", error=str(e))

    async def type_like_human(self, element, text: str, speed: str = "normal"):
        """Type text with human-like delays"""
        min_delay, max_delay = self.typing_speeds.get(speed, self.typing_speeds["normal"])
        
        for char in text:
            element.send_keys(char)
            await asyncio.sleep(random.uniform(min_delay, max_delay))

    def generate_session_fingerprint(self) -> Dict[str, Any]:
        """Generate a unique session fingerprint"""
        return {
            'user_agent': self.user_agent.random,
            'viewport': random.choice(self.viewport_sizes),
            'timezone': random.choice([
                'Africa/Johannesburg', 'GMT+2', 'UTC+2'
            ]),
            'language': random.choice(['en-US', 'en-GB', 'en-ZA']),
            'color_depth': random.choice([24, 32]),
            'pixel_ratio': random.choice([1, 1.25, 1.5, 2]),
            'session_id': f"sess_{int(time.time())}_{random.randint(1000, 9999)}"
        }

    async def wait_for_element(self, 
                              driver_or_page: Union[webdriver.Chrome, Page],
                              selector: str,
                              timeout: int = 10) -> Optional[Any]:
        """Wait for element to be present"""
        
        if isinstance(driver_or_page, webdriver.Chrome):
            try:
                element = WebDriverWait(driver_or_page, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                return element
            except Exception:
                return None
        else:
            try:
                await driver_or_page.wait_for_selector(selector, timeout=timeout * 1000)
                return await driver_or_page.query_selector(selector)
            except Exception:
                return None

    async def random_delay(self, min_delay: float = 1.0, max_delay: float = 5.0):
        """Add random delay between requests"""
        delay = random.uniform(min_delay, max_delay)
        logger.debug("Adding random delay", delay=delay)
        await asyncio.sleep(delay)