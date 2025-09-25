import logging
from ingestion.books_toscrape import BooksToScrapeScraper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def debug_parser():
    scraper = BooksToScrapeScraper()

    categories = scraper.get_all_categories()
    if not categories:
        logger.error("No categories found")
        return

    category_url = categories[0]['url']
    raw_pages = scraper.scrape_category(category_url, max_pages=1)

    book_pages = [
        page for page in raw_pages
        if '/catalogue/' in page.source_url and '/category/' not in page.source_url
    ]

    if not book_pages:
        logger.error("No book detail pages found")
        return

    book_page = book_pages[0]

    logger.info(f"📖 Examining book page: {book_page.source_url}")
    book_data = scraper.parse_book_page(book_page.raw_content, book_page.source_url)
    
    if book_data:
        print("=" * 60)
        print("📊 PARSED BOOK DATA:")
        print("=" * 60)
        
        # Display main fields
        main_fields = ['title', 'authors', 'price_current', 'currency', 'availability', 
                      'rating', 'num_reviews', 'category', 'upc']
        
        for field in main_fields:
            value = book_data.get(field, 'N/A')
            print(f"{field:>15}: {value}")
        
        print(f"{'description':>15}: {book_data.get('description', 'N/A')[:100]}...")
        
        # Display product info
        print(f"\n{'product_info':>15}:")
        for key, value in book_data.get('product_info', {}).items():
            print(f"                 {key}: {value}")
            
    else:
        logger.error("Failed to parse book page")

if __name__ == "__main__":
    debug_parser()