# 🎰 Sports Betting Odds Scraper & Arbitrage Detector

An advanced, stealth-enabled sports betting odds scraper designed specifically for South African bookmakers, with built-in arbitrage opportunity detection and robust anti-detection measures.

## 🌟 Features

### 🎯 **Advanced Scraping Capabilities**
- **Multi-Engine Support**: Playwright, Selenium, and HTTP requests with automatic fallback
- **Stealth Technology**: Comprehensive anti-detection including:
  - Browser fingerprint randomization
  - Human-like behavior simulation
  - Proxy rotation and health monitoring
  - Rate limiting with adaptive delays
  - CloudFlare challenge handling
- **Robust Error Handling**: Automatic retries, graceful degradation, and comprehensive logging

### 🏟️ **Supported Bookmakers** 
- **Hollywoodbets** ✅
- **Betway** ✅
- **Supabets** 🚧 (Coming Soon)
- **Playabets** 🚧 (Coming Soon)
- **Playbets** 🚧 (Coming Soon)
- **YesBet** 🚧 (Coming Soon)

### 💰 **Arbitrage Detection**
- **Real-time Analysis**: Automatically detect profitable arbitrage opportunities
- **Advanced Calculations**: Support for 2-way and 3-way arbitrage scenarios
- **Risk Assessment**: Confidence scoring and risk level categorization
- **Optimal Staking**: Calculate precise stake distribution for maximum profit

### 📊 **Sports Coverage**
- **Soccer/Football** ⚽
- **Rugby** 🏉
- **Cricket** 🏏
- **Tennis** 🎾
- **Basketball** 🏀
- **American Football** 🏈

### 🗄️ **Data Management**
- **Supabase Integration**: Cloud-native PostgreSQL database
- **Real-time Storage**: Immediate odds storage with versioning
- **Performance Monitoring**: Comprehensive logging and metrics
- **Data Cleanup**: Automatic removal of stale data

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Supabase account (free tier available)
- Chrome/Chromium browser (for advanced scraping)

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/your-username/sports-betting-scraper.git
cd sports-betting-scraper

# Install dependencies
pip install -r requirements.txt

# Install browser for Playwright (optional but recommended)
playwright install chromium
```

### 2. Database Setup

1. **Create Supabase Project**:
   - Go to [supabase.com](https://supabase.com)
   - Create a new project
   - Note your project URL and API keys

2. **Setup Database Schema**:
   - Go to SQL Editor in Supabase dashboard
   - Copy and paste contents of `database_schema.sql`
   - Execute the SQL

3. **Configure Environment**:
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env with your Supabase credentials
   nano .env
   ```

### 3. Configuration

Edit `.env` file with your settings:

```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_SERVICE_KEY=your-service-role-key

# Scraper Settings
SCRAPER_DELAY_MIN=2
SCRAPER_DELAY_MAX=8
MAX_CONCURRENT_REQUESTS=3
HEADLESS_MODE=true

# Arbitrage Settings
MIN_ARBITRAGE_PERCENTAGE=1.0
MAX_STAKE_AMOUNT=1000
```

### 4. Test Installation

```bash
# Test system functionality
python cli.py test

# Check system status
python cli.py status
```

## 📖 Usage

### Command Line Interface

The scraper includes a comprehensive CLI for easy operation:

```bash
# Show all available commands
python cli.py --help

# Run single scraping cycle
python cli.py scrape

# Run continuous scraping (every 30 minutes)
python cli.py scrape --continuous --interval 30

# Find arbitrage opportunities
python cli.py arbitrage --detect --min-profit 1.5

# Show current arbitrage opportunities
python cli.py arbitrage --limit 5

# Check system status
python cli.py status

# Setup database (first time)
python cli.py setup
```

### Programmatic Usage

```python
import asyncio
from src.main import OddsScrapingOrchestrator

async def main():
    # Initialize orchestrator
    orchestrator = OddsScrapingOrchestrator()
    await orchestrator.initialize()
    
    try:
        # Run single scraping cycle
        result = await orchestrator.run_full_scraping_cycle()
        
        if result['success']:
            print(f"Scraped {result['odds_results']['total_odds']} odds")
            print(f"Found {result['arbitrage_results']['opportunities_found']} arbitrage opportunities")
        
        # Get live arbitrage opportunities
        opportunities = await orchestrator.get_live_arbitrage_opportunities()
        for opp in opportunities:
            print(f"Arbitrage: {opp['profit_percentage']}% profit")
            
    finally:
        await orchestrator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🔧 Advanced Configuration

### Proxy Configuration

For enhanced anonymity, configure proxy rotation:

```env
PROXY_ENABLED=true
PROXY_LIST=proxy1:port:user:pass,proxy2:port:user:pass
```

### Stealth Settings

Customize anti-detection measures:

```env
# Browser settings
HEADLESS_MODE=true
BROWSER_EXECUTABLE_PATH=/path/to/chrome

# Rate limiting
SCRAPER_DELAY_MIN=1
SCRAPER_DELAY_MAX=5
MAX_CONCURRENT_REQUESTS=2

# Request timeout
REQUEST_TIMEOUT=30
```

### Logging Configuration

```env
LOG_LEVEL=INFO
LOG_FILE=logs/scraper.log
LOG_JSON=false
```

## 📊 Database Schema

The system uses a comprehensive PostgreSQL schema:

### Core Tables

- **`bookmakers`**: Bookmaker configuration and metadata
- **`sport_events`**: Sports events with teams, dates, and leagues
- **`odds`**: Individual odds with confidence scores and metadata
- **`arbitrage_opportunities`**: Detected arbitrage opportunities with calculations
- **`scraper_logs`**: Performance logs and error tracking

### Key Views

- **`v_latest_odds`**: Most recent odds for each event/bookmaker combination
- **`v_active_arbitrage`**: Currently active arbitrage opportunities
- **`v_bookmaker_performance`**: Scraper performance metrics by bookmaker

## 🛡️ Anti-Detection Features

### Browser Fingerprinting Protection
- Randomized user agents and viewport sizes
- Realistic browser profiles and plugins
- Dynamic header rotation

### Human Behavior Simulation
- Random mouse movements and scrolling
- Variable typing speeds and delays
- Realistic page interaction patterns

### Network Security
- Proxy rotation with health monitoring
- Request throttling and burst protection
- CloudFlare challenge auto-solving

### Error Resilience
- Automatic retry with exponential backoff
- Graceful degradation between scraping methods
- Comprehensive error logging and tracking

## 📈 Performance Monitoring

### Metrics Tracked
- **Scraping Performance**: Odds per second, success rates, error counts
- **Arbitrage Detection**: Opportunities found, profit percentages, success rates
- **System Health**: Database connectivity, scraper status, proxy health

### Logging
- Structured logging with context preservation
- Performance metrics and timing data
- Error tracking with categorization
- Audit trails for all operations

## 🚨 Legal and Ethical Considerations

⚠️ **Important Notice**: This tool is for educational and research purposes only.

### Responsible Usage Guidelines

1. **Respect Terms of Service**: Always review and comply with bookmaker terms
2. **Rate Limiting**: Use reasonable delays to avoid overwhelming servers
3. **Legal Compliance**: Ensure usage complies with local laws and regulations
4. **Commercial Use**: Obtain proper licenses if using for commercial purposes
5. **Data Protection**: Handle scraped data responsibly and securely

### Recommended Practices

- Use conservative rate limits (2-5 second delays)
- Monitor for anti-bot measures and respect them
- Implement proper error handling and graceful failures
- Keep detailed logs for debugging and compliance
- Regular review of scraping practices and bookmaker policies

## 🛠️ Development

### Project Structure

```
sports-betting-scraper/
├── src/
│   ├── models/           # Pydantic data models
│   ├── database/         # Supabase integration
│   ├── scrapers/         # Bookmaker-specific scrapers
│   ├── arbitrage/        # Arbitrage detection logic
│   ├── utils/            # Utilities (stealth, logging, etc.)
│   └── main.py          # Main orchestrator
├── cli.py               # Command-line interface
├── requirements.txt     # Python dependencies
├── database_schema.sql  # Database schema
├── .env.example        # Environment template
└── README.md           # This file
```

### Adding New Bookmakers

1. **Create Scraper Class**:
   ```python
   # src/scrapers/newbookmaker_scraper.py
   from .base_scraper import BaseScraper
   
   class NewBookmakerScraper(BaseScraper):
       def __init__(self, stealth_scraper, db_manager):
           super().__init__(
               bookmaker_name=BookmakerEnum.NEWBOOKMAKER,
               base_url="https://newbookmaker.com",
               stealth_scraper=stealth_scraper,
               db_manager=db_manager
           )
           # Implement scraper-specific logic
   ```

2. **Update Models**:
   ```python
   # Add to BookmakerEnum in src/models/odds.py
   class BookmakerEnum(str, Enum):
       NEWBOOKMAKER = "newbookmaker"
   ```

3. **Register Scraper**:
   ```python
   # Add to main.py
   scraper_classes = {
       BookmakerEnum.NEWBOOKMAKER: NewBookmakerScraper,
   }
   ```

### Testing

```bash
# Run basic functionality tests
python cli.py test

# Test specific scraper
python -m pytest tests/test_scrapers.py

# Test arbitrage detection
python -m pytest tests/test_arbitrage.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include unit tests for new features
- Update documentation as needed
- Respect rate limiting in test environments

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Supabase** for providing excellent database infrastructure
- **Playwright** and **Selenium** teams for robust browser automation
- **South African betting community** for inspiration and feedback

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-username/sports-betting-scraper/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-username/sports-betting-scraper/discussions)
- **Email**: your-email@example.com

---

**⚠️ Disclaimer**: This software is provided "as is" without warranty. Users are responsible for complying with all applicable laws and terms of service. The authors are not liable for any misuse or legal consequences arising from the use of this software.
