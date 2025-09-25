#!/usr/bin/env python3
"""
Direct import test
"""

# Add src to path first thing
import sys
sys.path.insert(0, '/opt/airflow/src')

# Now try direct imports
try:
    # Import the module directly
    from utils.schemas import SourceType
    print(f"✅ SourceType imported: {SourceType.BOOKS_TO_SCRAPE}")
    
    from utils.config import settings
    print("✅ settings imported")
    
    from ingestion.books_toscrape import BooksToScrapeScraper
    print("✅ BooksToScrapeScraper imported")
    
    # Test it
    scraper = BooksToScrapeScraper()
    print(f"✅ Scraper created with base URL: {scraper.BASE_URL}")
    
except Exception as e:
    print(f"❌ Import failed: {e}")
    print("\nDebugging info:")
    
    # Check what's in the paths
    import os
    print(f"Current directory: {os.getcwd()}")
    print(f"src exists: {os.path.exists('/opt/airflow/src')}")
    print(f"utils exists: {os.path.exists('/opt/airflow/src/utils')}")
    print(f"schemas.py exists: {os.path.exists('/opt/airflow/src/utils/schemas.py')}")
    
    if os.path.exists('/opt/airflow/src/utils'):
        print("Contents of utils:")
        for item in os.listdir('/opt/airflow/src/utils'):
            print(f"  {item}")