import os
import sys
import logging
from typing import Any, Dict
import structlog
from datetime import datetime


def setup_logging(log_level: str = "INFO", 
                 log_file: str = None,
                 json_format: bool = False) -> None:
    """
    Configure structured logging for the sports betting scraper
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        json_format: Whether to use JSON formatting
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )
    
    # Configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="ISO"),
    ]
    
    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper())
        ),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Set up file logging if specified
    if log_file:
        setup_file_logging(log_file, log_level, json_format)
    
    # Configure logging for third-party libraries
    configure_third_party_loggers()
    
    logger = structlog.get_logger(__name__)
    logger.info("Logging configured", 
               level=log_level, 
               file=log_file, 
               json_format=json_format)


def setup_file_logging(log_file: str, log_level: str, json_format: bool) -> None:
    """Set up file logging with rotation"""
    from logging.handlers import RotatingFileHandler
    
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Set up rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    
    if json_format:
        formatter = logging.Formatter('%(message)s')
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Add to root logger
    logging.getLogger().addHandler(file_handler)


def configure_third_party_loggers() -> None:
    """Configure logging levels for third-party libraries"""
    
    # Selenium logging
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # HTTP libraries
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Database libraries
    logging.getLogger('asyncpg').setLevel(logging.WARNING)
    logging.getLogger('supabase').setLevel(logging.WARNING)
    
    # Playwright
    logging.getLogger('playwright').setLevel(logging.WARNING)
    
    # Other libraries
    logging.getLogger('fake_useragent').setLevel(logging.ERROR)


class ScrapeLoggerAdapter:
    """Custom logger adapter for scraping operations"""
    
    def __init__(self, logger_name: str):
        self.logger = structlog.get_logger(logger_name)
        self.session_context = {}
    
    def set_session_context(self, **kwargs):
        """Set session-level context for all log messages"""
        self.session_context.update(kwargs)
    
    def clear_session_context(self):
        """Clear session context"""
        self.session_context.clear()
    
    def _log_with_context(self, method_name: str, message: str, **kwargs):
        """Log with combined session and message context"""
        combined_context = {**self.session_context, **kwargs}
        getattr(self.logger, method_name)(message, **combined_context)
    
    def debug(self, message: str, **kwargs):
        self._log_with_context('debug', message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self._log_with_context('info', message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log_with_context('warning', message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log_with_context('error', message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log_with_context('critical', message, **kwargs)


class PerformanceLogger:
    """Logger for tracking performance metrics"""
    
    def __init__(self):
        self.logger = structlog.get_logger("performance")
        self.metrics = {}
    
    def start_timer(self, operation: str):
        """Start timing an operation"""
        self.metrics[operation] = {
            'start_time': datetime.now(),
            'status': 'running'
        }
    
    def end_timer(self, operation: str, success: bool = True, **extra_data):
        """End timing an operation and log results"""
        if operation not in self.metrics:
            self.logger.warning("Timer not found", operation=operation)
            return
        
        start_time = self.metrics[operation]['start_time']
        duration = (datetime.now() - start_time).total_seconds()
        
        self.logger.info(
            "Operation completed",
            operation=operation,
            duration_seconds=round(duration, 3),
            success=success,
            **extra_data
        )
        
        # Clean up
        del self.metrics[operation]
    
    def log_scrape_stats(self, 
                        bookmaker: str,
                        sport: str,
                        events_found: int,
                        odds_extracted: int,
                        errors: int,
                        duration: float):
        """Log scraping statistics"""
        self.logger.info(
            "Scrape statistics",
            bookmaker=bookmaker,
            sport=sport,
            events_found=events_found,
            odds_extracted=odds_extracted,
            errors=errors,
            duration_seconds=round(duration, 3),
            odds_per_second=round(odds_extracted / max(duration, 0.1), 2) if duration > 0 else 0
        )
    
    def log_arbitrage_stats(self,
                           events_analyzed: int,
                           opportunities_found: int,
                           best_profit_percentage: float,
                           analysis_duration: float):
        """Log arbitrage analysis statistics"""
        self.logger.info(
            "Arbitrage analysis statistics",
            events_analyzed=events_analyzed,
            opportunities_found=opportunities_found,
            best_profit_percentage=round(best_profit_percentage, 4) if best_profit_percentage else 0,
            analysis_duration_seconds=round(analysis_duration, 3),
            opportunity_rate=round((opportunities_found / max(events_analyzed, 1)) * 100, 2)
        )


class ErrorTracker:
    """Track and categorize errors for monitoring"""
    
    def __init__(self):
        self.logger = structlog.get_logger("errors")
        self.error_counts = {}
    
    def track_error(self, 
                   error_type: str,
                   error_message: str,
                   component: str,
                   bookmaker: str = None,
                   **extra_context):
        """Track an error occurrence"""
        
        error_key = f"{component}:{error_type}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        self.logger.error(
            "Error tracked",
            error_type=error_type,
            error_message=error_message,
            component=component,
            bookmaker=bookmaker,
            error_count=self.error_counts[error_key],
            **extra_context
        )
    
    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of error counts"""
        return self.error_counts.copy()
    
    def reset_error_counts(self):
        """Reset error tracking"""
        self.error_counts.clear()


def get_scraper_logger(component_name: str) -> ScrapeLoggerAdapter:
    """Get a scraper-specific logger"""
    return ScrapeLoggerAdapter(f"scraper.{component_name}")


def get_performance_logger() -> PerformanceLogger:
    """Get performance logger instance"""
    return PerformanceLogger()


def get_error_tracker() -> ErrorTracker:
    """Get error tracker instance"""
    return ErrorTracker()