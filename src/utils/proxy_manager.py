import os
import random
import asyncio
import aiohttp
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


class ProxyManager:
    """Advanced proxy manager with health monitoring and rotation"""
    
    def __init__(self):
        self.proxy_list: List[str] = []
        self.current_proxy_index = 0
        self.proxy_health: Dict[str, Dict[str, Any]] = {}
        self.rotation_enabled = True
        self.health_check_interval = 300  # 5 minutes
        self.max_failures = 3
        
        # Load proxies from environment or file
        self._load_proxies()

    def _load_proxies(self):
        """Load proxy list from environment variables or file"""
        proxy_string = os.getenv('PROXY_LIST', '')
        
        if proxy_string:
            # Format: proxy1:port:user:pass,proxy2:port:user:pass
            self.proxy_list = [p.strip() for p in proxy_string.split(',') if p.strip()]
        else:
            # Try to load from file
            proxy_file = 'proxies.txt'
            if os.path.exists(proxy_file):
                try:
                    with open(proxy_file, 'r') as f:
                        self.proxy_list = [line.strip() for line in f if line.strip()]
                except Exception as e:
                    logger.warning("Failed to load proxy file", error=str(e))
        
        # Initialize proxy health tracking
        for proxy in self.proxy_list:
            self.proxy_health[proxy] = {
                'failures': 0,
                'last_success': None,
                'last_failure': None,
                'response_time': None,
                'is_active': True
            }
        
        logger.info("Proxy manager initialized", proxy_count=len(self.proxy_list))

    async def initialize(self):
        """Initialize proxy manager and start health monitoring"""
        if self.proxy_list:
            # Start background health monitoring
            asyncio.create_task(self._health_monitor())
        else:
            logger.warning("No proxies configured, running without proxy rotation")

    async def get_proxy(self) -> Optional[str]:
        """Get next available proxy"""
        if not self.proxy_list:
            return None
        
        # Filter active proxies
        active_proxies = [
            proxy for proxy in self.proxy_list 
            if self.proxy_health[proxy]['is_active']
        ]
        
        if not active_proxies:
            logger.warning("No active proxies available")
            return None
        
        if self.rotation_enabled:
            # Round-robin selection with health consideration
            proxy = self._select_best_proxy(active_proxies)
        else:
            # Use current proxy if available
            current_proxy = self.proxy_list[self.current_proxy_index]
            proxy = current_proxy if current_proxy in active_proxies else active_proxies[0]
        
        logger.debug("Selected proxy", proxy=self._mask_proxy(proxy))
        return self._format_proxy_url(proxy)

    def _select_best_proxy(self, active_proxies: List[str]) -> str:
        """Select the best proxy based on health metrics"""
        proxy_scores = []
        
        for proxy in active_proxies:
            health = self.proxy_health[proxy]
            
            # Calculate score based on failures and response time
            failure_score = max(0, 10 - health['failures'])
            time_score = 10 if health['response_time'] is None else max(0, 10 - health['response_time'])
            
            # Bonus for recent success
            recency_score = 0
            if health['last_success']:
                time_diff = (datetime.now() - health['last_success']).total_seconds()
                recency_score = max(0, 5 - (time_diff / 3600))  # Bonus for success within last hour
            
            total_score = failure_score + time_score + recency_score
            proxy_scores.append((proxy, total_score))
        
        # Sort by score and add some randomness
        proxy_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Select from top 3 proxies randomly
        top_proxies = proxy_scores[:min(3, len(proxy_scores))]
        return random.choice(top_proxies)[0]

    def _format_proxy_url(self, proxy: str) -> str:
        """Format proxy string as URL"""
        parts = proxy.split(':')
        
        if len(parts) == 2:
            # IP:Port
            return f"http://{parts[0]}:{parts[1]}"
        elif len(parts) == 4:
            # IP:Port:User:Pass
            return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        else:
            return f"http://{proxy}"

    def _mask_proxy(self, proxy: str) -> str:
        """Mask proxy credentials for logging"""
        parts = proxy.split(':')
        if len(parts) == 4:
            return f"{parts[0]}:{parts[1]}:***:***"
        return proxy

    async def mark_proxy_failed(self, proxy: Optional[str] = None):
        """Mark current or specified proxy as failed"""
        if not proxy and self.proxy_list:
            proxy = self.proxy_list[self.current_proxy_index]
        
        if proxy and proxy in self.proxy_health:
            health = self.proxy_health[proxy]
            health['failures'] += 1
            health['last_failure'] = datetime.now()
            
            if health['failures'] >= self.max_failures:
                health['is_active'] = False
                logger.warning("Proxy marked as inactive due to failures", 
                             proxy=self._mask_proxy(proxy),
                             failures=health['failures'])
            
            # Rotate to next proxy
            if self.rotation_enabled:
                self._rotate_proxy()

    async def mark_proxy_success(self, proxy: Optional[str] = None, response_time: Optional[float] = None):
        """Mark current or specified proxy as successful"""
        if not proxy and self.proxy_list:
            proxy = self.proxy_list[self.current_proxy_index]
        
        if proxy and proxy in self.proxy_health:
            health = self.proxy_health[proxy]
            health['failures'] = max(0, health['failures'] - 1)  # Reduce failure count
            health['last_success'] = datetime.now()
            
            if response_time:
                health['response_time'] = response_time
            
            # Reactivate if it was inactive
            if not health['is_active'] and health['failures'] < self.max_failures:
                health['is_active'] = True
                logger.info("Proxy reactivated", proxy=self._mask_proxy(proxy))

    def _rotate_proxy(self):
        """Rotate to next proxy"""
        if len(self.proxy_list) > 1:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            logger.debug("Rotated to next proxy", 
                        index=self.current_proxy_index,
                        proxy=self._mask_proxy(self.proxy_list[self.current_proxy_index]))

    async def _health_monitor(self):
        """Background task to monitor proxy health"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._check_proxy_health()
            except Exception as e:
                logger.error("Proxy health monitor error", error=str(e))

    async def _check_proxy_health(self):
        """Check health of all proxies"""
        logger.info("Starting proxy health check")
        
        test_url = "http://httpbin.org/ip"
        timeout = aiohttp.ClientTimeout(total=10)
        
        for proxy in self.proxy_list:
            try:
                proxy_url = self._format_proxy_url(proxy)
                start_time = asyncio.get_event_loop().time()
                
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(test_url, proxy=proxy_url) as response:
                        if response.status == 200:
                            response_time = asyncio.get_event_loop().time() - start_time
                            await self.mark_proxy_success(proxy, response_time)
                            logger.debug("Proxy health check passed", 
                                       proxy=self._mask_proxy(proxy),
                                       response_time=response_time)
                        else:
                            await self.mark_proxy_failed(proxy)
                            
            except Exception as e:
                await self.mark_proxy_failed(proxy)
                logger.debug("Proxy health check failed", 
                           proxy=self._mask_proxy(proxy),
                           error=str(e))

    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy statistics"""
        active_count = sum(1 for health in self.proxy_health.values() if health['is_active'])
        inactive_count = len(self.proxy_list) - active_count
        
        avg_response_time = None
        response_times = [
            health['response_time'] 
            for health in self.proxy_health.values() 
            if health['response_time'] is not None
        ]
        
        if response_times:
            avg_response_time = sum(response_times) / len(response_times)
        
        return {
            'total_proxies': len(self.proxy_list),
            'active_proxies': active_count,
            'inactive_proxies': inactive_count,
            'current_proxy_index': self.current_proxy_index,
            'average_response_time': avg_response_time,
            'rotation_enabled': self.rotation_enabled
        }

    def reset_proxy_health(self, proxy: Optional[str] = None):
        """Reset health status for specified proxy or all proxies"""
        if proxy and proxy in self.proxy_health:
            self.proxy_health[proxy] = {
                'failures': 0,
                'last_success': None,
                'last_failure': None,
                'response_time': None,
                'is_active': True
            }
            logger.info("Proxy health reset", proxy=self._mask_proxy(proxy))
        else:
            # Reset all proxies
            for proxy_addr in self.proxy_list:
                self.reset_proxy_health(proxy_addr)
            logger.info("All proxy health statuses reset")