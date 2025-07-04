#!/usr/bin/env python3
"""
Setup script for Sports Betting Odds Scraper & Arbitrage Detector
"""

import os
import subprocess
import sys
from pathlib import Path

def print_header():
    print("=" * 60)
    print("🎰 Sports Betting Odds Scraper Setup")
    print("=" * 60)

def print_step(step, description):
    print(f"\n📋 Step {step}: {description}")
    print("-" * 40)

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"Running: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    
    print(f"✅ Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    return True

def install_dependencies():
    """Install Python dependencies"""
    print("Installing Python dependencies...")
    
    # Check if pip is available
    if not run_command("pip --version", "Checking pip"):
        print("❌ pip is not available. Please install pip first.")
        return False
    
    # Install requirements
    if not run_command("pip install -r requirements.txt", "Installing dependencies"):
        print("❌ Failed to install dependencies")
        return False
    
    print("✅ Dependencies installed successfully")
    return True

def install_playwright():
    """Install Playwright browsers"""
    print("Installing Playwright browsers...")
    
    if not run_command("playwright install chromium", "Installing Chromium"):
        print("⚠️  Warning: Playwright browser installation failed")
        print("    You can still use HTTP scraping, but browser-based scraping won't work")
        return False
    
    print("✅ Playwright browsers installed")
    return True

def setup_environment():
    """Setup environment configuration"""
    env_example = Path(".env.example")
    env_file = Path(".env")
    
    if not env_example.exists():
        print("❌ .env.example file not found")
        return False
    
    if env_file.exists():
        print("⚠️  .env file already exists")
        response = input("Do you want to overwrite it? (y/N): ")
        if response.lower() != 'y':
            print("Keeping existing .env file")
            return True
    
    # Copy .env.example to .env
    try:
        with open(env_example, 'r') as src:
            content = src.read()
        
        with open(env_file, 'w') as dst:
            dst.write(content)
        
        print("✅ Environment file created (.env)")
        print("📝 Please edit .env file with your Supabase credentials")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = [
        "logs",
        "data",
        "temp"
    ]
    
    for directory in directories:
        try:
            Path(directory).mkdir(exist_ok=True)
            print(f"📁 Created directory: {directory}")
        except Exception as e:
            print(f"❌ Failed to create directory {directory}: {e}")

def test_installation():
    """Test the installation"""
    print("Testing installation...")
    
    try:
        # Test imports
        sys.path.insert(0, 'src')
        from src.models import BookmakerEnum, SportType
        from src.utils import setup_logging
        from src.database import SupabaseManager
        
        print("✅ Core modules imported successfully")
        
        # Test CLI
        if run_command("python cli.py --help", "Testing CLI"):
            print("✅ CLI is working")
        else:
            print("❌ CLI test failed")
            return False
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def show_next_steps():
    """Show next steps to user"""
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next Steps:")
    print("1. Edit .env file with your Supabase credentials")
    print("2. Run database setup: python cli.py setup")
    print("3. Test the system: python cli.py test")
    print("4. Start scraping: python cli.py scrape")
    
    print("\n📚 Quick Commands:")
    print("  python cli.py --help          # Show all commands")
    print("  python cli.py status          # Check system status") 
    print("  python cli.py scrape          # Run single scrape")
    print("  python cli.py arbitrage       # Find arbitrage opportunities")
    
    print("\n📖 For detailed documentation, see README.md")

def main():
    """Main setup function"""
    print_header()
    
    # Step 1: Check Python version
    print_step(1, "Checking Python version")
    if not check_python_version():
        sys.exit(1)
    
    # Step 2: Install dependencies
    print_step(2, "Installing dependencies")
    if not install_dependencies():
        print("❌ Setup failed at dependency installation")
        sys.exit(1)
    
    # Step 3: Install Playwright (optional)
    print_step(3, "Installing Playwright browsers (optional)")
    install_playwright()  # Don't fail on this
    
    # Step 4: Setup environment
    print_step(4, "Setting up environment configuration")
    if not setup_environment():
        print("❌ Setup failed at environment configuration")
        sys.exit(1)
    
    # Step 5: Create directories
    print_step(5, "Creating directories")
    create_directories()
    
    # Step 6: Test installation
    print_step(6, "Testing installation")
    if not test_installation():
        print("❌ Installation test failed")
        print("   You may need to check your Python environment")
        sys.exit(1)
    
    # Show next steps
    show_next_steps()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        sys.exit(1)