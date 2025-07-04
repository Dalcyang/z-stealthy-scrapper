#!/usr/bin/env python3
"""
Sports Betting Odds Scraper CLI

Command-line interface for managing the sports betting odds scraper system.
"""

import os
import sys
import asyncio
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.main import OddsScrapingOrchestrator
from src.models import SportType, BookmakerEnum
from src.utils import setup_logging
from src.database import SupabaseManager
from src.arbitrage import ArbitrageDetector


def setup_cli_logging():
    """Setup logging for CLI"""
    setup_logging(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        log_file=None,  # CLI logs to console only
        json_format=False
    )


def print_header():
    """Print CLI header"""
    print("=" * 60)
    print("🎰 Sports Betting Odds Scraper & Arbitrage Detector")
    print("=" * 60)


def print_separator():
    """Print separator line"""
    print("-" * 60)


async def cmd_scrape(args):
    """Run odds scraping"""
    print_header()
    print("📊 Starting odds scraping...")
    print_separator()
    
    orchestrator = OddsScrapingOrchestrator()
    
    try:
        await orchestrator.initialize()
        
        if args.continuous:
            print(f"🔄 Running continuous scraping (interval: {args.interval} minutes)")
            await orchestrator.run_continuous(args.interval)
        else:
            print("🔀 Running single scraping cycle...")
            result = await orchestrator.run_full_scraping_cycle()
            
            if result.get('success'):
                print("✅ Scraping completed successfully!")
                print(f"📈 Odds scraped: {result['odds_results']['total_odds']}")
                print(f"🏟️  Events found: {result['odds_results']['total_events']}")
                print(f"💰 Arbitrage opportunities: {result['arbitrage_results']['opportunities_found']}")
            else:
                print(f"❌ Scraping failed: {result.get('error')}")
                
    except KeyboardInterrupt:
        print("\n🛑 Scraping interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await orchestrator.shutdown()


async def cmd_arbitrage(args):
    """Find arbitrage opportunities"""
    print_header()
    print("💰 Finding arbitrage opportunities...")
    print_separator()
    
    try:
        db_manager = SupabaseManager()
        detector = ArbitrageDetector(
            db_manager=db_manager,
            min_profit_percentage=args.min_profit
        )
        
        if args.detect:
            print("🔍 Detecting new arbitrage opportunities...")
            opportunities = await detector.detect_all_arbitrage_opportunities()
            
            if opportunities:
                print(f"✅ Found {len(opportunities)} arbitrage opportunities!")
                await detector.save_opportunities(opportunities)
                print("💾 Opportunities saved to database")
            else:
                print("❌ No arbitrage opportunities found")
        
        # Show current opportunities
        current_opportunities = await detector.get_best_opportunities(
            limit=args.limit, 
            min_profit=args.min_profit
        )
        
        if current_opportunities:
            print(f"\n🎯 Top {len(current_opportunities)} Arbitrage Opportunities:")
            print_separator()
            
            for i, opp in enumerate(current_opportunities, 1):
                print(f"{i}. {opp['home_team']} vs {opp['away_team']}")
                print(f"   💰 Profit: {opp['profit_percentage']:.2f}%")
                print(f"   🏟️  League: {opp.get('league', 'Unknown')}")
                print(f"   📅 Date: {opp['event_date']}")
                print(f"   🎲 Bet Type: {opp['bet_type']}")
                print()
        else:
            print("❌ No current arbitrage opportunities found")
            
    except Exception as e:
        print(f"❌ Error: {e}")


async def cmd_status(args):
    """Show system status"""
    print_header()
    print("📊 System Status")
    print_separator()
    
    try:
        db_manager = SupabaseManager()
        
        # Check database connectivity
        try:
            bookmakers = await db_manager.get_all_bookmakers()
            print(f"✅ Database: Connected ({len(bookmakers)} bookmakers configured)")
        except Exception as e:
            print(f"❌ Database: Connection failed - {e}")
            return
        
        # Recent odds statistics
        cutoff_time = datetime.now() - timedelta(hours=24)
        recent_odds = await db_manager.execute_query(
            'odds', 'select',
            filters={'last_updated': {'gte': cutoff_time.isoformat()}}
        )
        
        print(f"📈 Recent Odds (24h): {len(recent_odds) if recent_odds else 0}")
        
        # Recent events
        recent_events = await db_manager.get_upcoming_events(limit=100)
        print(f"🏟️  Upcoming Events: {len(recent_events) if recent_events else 0}")
        
        # Active arbitrage opportunities
        detector = ArbitrageDetector(db_manager)
        opportunities = await detector.get_best_opportunities(limit=100)
        print(f"💰 Active Arbitrage: {len(opportunities) if opportunities else 0}")
        
        # Bookmaker status
        print("\n📚 Bookmaker Status:")
        for bookmaker in bookmakers:
            # Get recent odds for this bookmaker
            bookmaker_odds = await db_manager.execute_query(
                'odds', 'select',
                filters={
                    'bookmaker_id': bookmaker['id'],
                    'last_updated': {'gte': cutoff_time.isoformat()}
                }
            )
            
            odds_count = len(bookmaker_odds) if bookmaker_odds else 0
            status = "🟢 Active" if odds_count > 0 else "🔴 Inactive"
            print(f"  {bookmaker['display_name']}: {status} ({odds_count} odds)")
            
    except Exception as e:
        print(f"❌ Error getting status: {e}")


async def cmd_setup(args):
    """Setup database and initial configuration"""
    print_header()
    print("🔧 Setting up database...")
    print_separator()
    
    try:
        # Check if database schema file exists
        schema_file = "database_schema.sql"
        if not os.path.exists(schema_file):
            print(f"❌ Database schema file not found: {schema_file}")
            print("Please ensure the database_schema.sql file exists in the project root.")
            return
        
        # Read and display schema info
        with open(schema_file, 'r') as f:
            schema_content = f.read()
        
        table_count = schema_content.count('CREATE TABLE')
        view_count = schema_content.count('CREATE OR REPLACE VIEW')
        function_count = schema_content.count('CREATE OR REPLACE FUNCTION')
        
        print(f"📋 Schema includes:")
        print(f"  📊 Tables: {table_count}")
        print(f"  👁️  Views: {view_count}")
        print(f"  ⚙️  Functions: {function_count}")
        
        print("\n🚀 To setup your Supabase database:")
        print("1. Create a new Supabase project at https://supabase.com")
        print("2. Go to SQL Editor in your Supabase dashboard")
        print("3. Copy and run the contents of database_schema.sql")
        print("4. Copy .env.example to .env and fill in your Supabase credentials")
        
        print("\n📝 Required environment variables:")
        print("  SUPABASE_URL=https://your-project.supabase.co")
        print("  SUPABASE_KEY=your-supabase-anon-key")
        print("  SUPABASE_SERVICE_KEY=your-service-role-key")
        
        # Test connection if env vars are set
        if os.getenv('SUPABASE_URL') and os.getenv('SUPABASE_KEY'):
            print("\n🔍 Testing database connection...")
            try:
                db_manager = SupabaseManager()
                bookmakers = await db_manager.get_all_bookmakers()
                print(f"✅ Database connection successful! Found {len(bookmakers)} bookmakers.")
            except Exception as e:
                print(f"❌ Database connection failed: {e}")
        else:
            print("\n⚠️  Environment variables not set. Please configure .env file first.")
            
    except Exception as e:
        print(f"❌ Setup error: {e}")


async def cmd_test(args):
    """Test scraper functionality"""
    print_header()
    print("🧪 Testing scraper functionality...")
    print_separator()
    
    try:
        # Test basic imports
        print("📦 Testing imports... ✅")
        
        # Test database connection
        print("🔍 Testing database connection...")
        db_manager = SupabaseManager()
        bookmakers = await db_manager.get_all_bookmakers()
        print(f"✅ Database connected ({len(bookmakers)} bookmakers)")
        
        # Test scraper initialization
        print("🤖 Testing scraper initialization...")
        orchestrator = OddsScrapingOrchestrator()
        await orchestrator.initialize()
        print(f"✅ Scrapers initialized ({len(orchestrator.scrapers)})")
        
        # Test arbitrage detector
        print("💰 Testing arbitrage detector...")
        detector = ArbitrageDetector(db_manager)
        opportunities = await detector.get_best_opportunities(limit=1)
        print(f"✅ Arbitrage detector working")
        
        print("\n🎉 All tests passed! System is ready to use.")
        
        await orchestrator.shutdown()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Sports Betting Odds Scraper & Arbitrage Detector",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Run odds scraping')
    scrape_parser.add_argument('--continuous', action='store_true', 
                              help='Run continuously (default: single run)')
    scrape_parser.add_argument('--interval', type=int, default=30,
                              help='Interval between runs in minutes (default: 30)')
    
    # Arbitrage command
    arb_parser = subparsers.add_parser('arbitrage', help='Find arbitrage opportunities')
    arb_parser.add_argument('--detect', action='store_true',
                           help='Detect new opportunities (default: show existing)')
    arb_parser.add_argument('--min-profit', type=float, default=1.0,
                           help='Minimum profit percentage (default: 1.0)')
    arb_parser.add_argument('--limit', type=int, default=10,
                           help='Maximum opportunities to show (default: 10)')
    
    # Status command
    subparsers.add_parser('status', help='Show system status')
    
    # Setup command
    subparsers.add_parser('setup', help='Setup database and configuration')
    
    # Test command
    subparsers.add_parser('test', help='Test system functionality')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    setup_cli_logging()
    
    # Map commands to functions
    commands = {
        'scrape': cmd_scrape,
        'arbitrage': cmd_arbitrage,
        'status': cmd_status,
        'setup': cmd_setup,
        'test': cmd_test
    }
    
    # Run the command
    try:
        asyncio.run(commands[args.command](args))
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()