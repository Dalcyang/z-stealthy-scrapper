import asyncio
import time
from typing import Optional
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


class RateLimiter:
    """Advanced rate limiter with adaptive delays and burst protection"""
    
    def __init__(self, 
                 requests_per_second: float = 1.0,
                 burst_size: int = 5,
                 adaptive: bool = True):
        
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.adaptive = adaptive
        
        # Rate limiting state
        self.last_request_time = 0.0
        self.request_times = []
        self.current_delay = 1.0 / requests_per_second
        
        # Adaptive rate limiting
        self.success_count = 0
        self.failure_count = 0
        self.adjustment_threshold = 10
        
        # Semaphore for burst control
        self.semaphore: Optional[asyncio.Semaphore] = None

    async def initialize(self):
        """Initialize rate limiter"""
        self.semaphore = asyncio.Semaphore(self.burst_size)
        logger.info("Rate limiter initialized", 
                   requests_per_second=self.requests_per_second,
                   burst_size=self.burst_size)

    async def acquire(self) -> None:
        """Acquire permission to make a request"""
        if not self.semaphore:
            await self.initialize()
        
        async with self.semaphore:
            await self._apply_rate_limit()
            
            # Record request time
            current_time = time.time()
            self.request_times.append(current_time)
            self.last_request_time = current_time
            
            # Clean old request times (keep only last minute)
            cutoff_time = current_time - 60
            self.request_times = [t for t in self.request_times if t > cutoff_time]

    async def _apply_rate_limit(self) -> None:
        """Apply rate limiting logic"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.current_delay:
            sleep_time = self.current_delay - time_since_last
            logger.debug("Rate limiting applied", sleep_time=sleep_time)
            await asyncio.sleep(sleep_time)

    def record_success(self) -> None:
        """Record a successful request for adaptive rate limiting"""
        self.success_count += 1
        
        if self.adaptive and self.success_count % self.adjustment_threshold == 0:
            # Increase rate slightly on consistent success
            self.current_delay = max(0.5, self.current_delay * 0.95)
            self.requests_per_second = 1.0 / self.current_delay
            logger.info("Rate limit adjusted (faster)", 
                       new_delay=self.current_delay,
                       requests_per_second=self.requests_per_second)

    def record_failure(self) -> None:
        """Record a failed request for adaptive rate limiting"""
        self.failure_count += 1
        
        if self.adaptive:
            # Increase delay on failure
            self.current_delay = min(10.0, self.current_delay * 1.5)
            self.requests_per_second = 1.0 / self.current_delay
            logger.info("Rate limit adjusted (slower)", 
                       new_delay=self.current_delay,
                       requests_per_second=self.requests_per_second)

    def get_current_rate(self) -> float:
        """Get current requests per second"""
        return self.requests_per_second

    def get_request_count(self, window_seconds: int = 60) -> int:
        """Get number of requests in the last N seconds"""
        current_time = time.time()
        cutoff_time = current_time - window_seconds
        return len([t for t in self.request_times if t > cutoff_time])

    async def wait_for_rate_limit_reset(self, target_rate: float) -> None:
        """Wait until the rate drops below target"""
        while True:
            current_rate = self.get_request_count(60) / 60.0  # requests per second
            if current_rate <= target_rate:
                break
            
            await asyncio.sleep(1)
            logger.debug("Waiting for rate limit reset", 
                        current_rate=current_rate, 
                        target_rate=target_rate)