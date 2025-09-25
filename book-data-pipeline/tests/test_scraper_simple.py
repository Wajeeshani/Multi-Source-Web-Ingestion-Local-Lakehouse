# Create a simple test script tests/test_scraper_simple.py
from src.ingestion.books_toscrape import scrape_books_toscrape

if __name__ == "__main__":
    # Test with very limited scraping
    raw_books = scrape_books_toscrape(max_categories=1, max_pages_per_category=1)
    print(f"Scraped {len(raw_books)} raw pages")
    
    for book in raw_books[:2]:  # Show first 2 as sample
        print(f"Source: {book.source}")
        print(f"URL: {book.source_url}")
        print(f"Content length: {len(book.raw_content)}")
        print("---")