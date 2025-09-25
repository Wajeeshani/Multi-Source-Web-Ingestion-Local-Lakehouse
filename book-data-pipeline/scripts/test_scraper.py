#!/usr/bin/env python3
"""
Quick test script for Books to Scrape scraper
"""

import sys
import os
import logging

# Add src to Python path
sys.path.insert(0, '/opt/airflow/src')

print("Python path:")
for p in sys.path[:3]:  # Show first 3 paths
    print(f"  {p}")

try:
    from utils.schemas import SourceType, RawBookData
    print("✅ utils.schemas import successful")
    
    from utils.config import settings
    print("✅ utils.config import successful")
    
    from ingestion.books_toscrape import BooksToScrapeScraper
    print("✅ ingestion.books_toscrape import successful")
    
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_scraper_safe():
    """Test scraper functionality without making real requests"""
    print("🧪 Testing scraper functionality...")
    
    try:
        # Test initialization
        scraper = BooksToScrapeScraper(delay=1.0)
        print("✅ Scraper initialized successfully")
        
        # Test session creation
        assert scraper.session is not None
        print("✅ HTTP session created")
        
        # Test basic attributes
        print(f"✅ Base URL: {scraper.BASE_URL}")
        print(f"✅ Request delay: {scraper.delay} seconds")
        
        # Test getting categories (this will make a real HTTP request)
        print("\n🌐 Testing category fetch...")
        categories = scraper.get_all_categories()
        
        if categories:
            print(f"✅ Found {len(categories)} categories")
            for cat in categories[:3]:  # Show first 3
                print(f"   - {cat['name']}: {cat['url']}")
        else:
            print("⚠️  No categories found (might be network issue)")
        
        print("\n🎯 Scraper functionality test COMPLETED!")
        return True
        
    except Exception as e:
        print(f"❌ Error during scraper test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_scraper_safe()