import requests
from typing import List, Dict, Optional
import time
import logging
from datetime import datetime
from urllib.parse import urljoin

from utils.schemas import RawBookData, SourceType
from utils.config import settings

logger = logging.getLogger(__name__)

class ITBookstoreAPI:
    """
    Client for IT Bookstore API (api.itbook.store)
    Provides access to IT/technology books
    """
    
    BASE_URL = "https://api.itbook.store/1.0/"
    ENDPOINTS = {
        'new': 'new',
        'search': 'search/',
        'book_detail': 'books/'
    }
    
    def __init__(self, delay: float = None):
        self.delay = delay or settings.request_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
    
    def get_new_books(self, page: int = 1) -> List[Dict]:
        """Get new books from the API"""
        try:
            url = urljoin(self.BASE_URL, self.ENDPOINTS['new'])
            params = {'page': page}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            books = data.get('books', [])
            
            logger.info(f"Retrieved {len(books)} new books from page {page}")
            return books
            
        except requests.RequestException as e:
            logger.error(f"Error fetching new books: {e}")
            return []
    
    def search_books(self, query: str, page: int = 1) -> List[Dict]:
        """Search books by query"""
        try:
            url = urljoin(self.BASE_URL, self.ENDPOINTS['search'] + query)
            params = {'page': page}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            books = data.get('books', [])
            
            logger.info(f"Found {len(books)} books for query '{query}' on page {page}")
            return books
            
        except requests.RequestException as e:
            logger.error(f"Error searching books: {e}")
            return []
    
    def get_book_details(self, isbn13: str) -> Optional[Dict]:
        """Get detailed information for a specific book"""
        try:
            url = urljoin(self.BASE_URL, self.ENDPOINTS['book_detail'] + isbn13)
            
            response = self.session.get(url)
            response.raise_for_status()
            
            book_data = response.json()
            
            # Add the ISBN13 to the response for consistency
            book_data['isbn13'] = isbn13
            
            logger.info(f"Retrieved details for book: {book_data.get('title', 'Unknown')}")
            return book_data
            
        except requests.RequestException as e:
            logger.error(f"Error fetching book details for {isbn13}: {e}")
            return None
    
    def hydrate_book_details(self, book_list: List[Dict]) -> List[Dict]:
        """Hydrate a list of books with full details"""
        hydrated_books = []
        
        for book in book_list:
            isbn13 = book.get('isbn13')
            if isbn13:
                details = self.get_book_details(isbn13)
                if details:
                    # Merge basic info with detailed info
                    merged_book = {**book, **details}
                    hydrated_books.append(merged_book)
                
                time.sleep(self.delay)  # Rate limiting
        
        logger.info(f"Hydrated {len(hydrated_books)} books with details")
        return hydrated_books
    
    def get_sample_books(self, max_books: int = 10) -> List[RawBookData]:
        """Get a sample of books for testing"""
        raw_books = []
        
        # Get new books
        new_books = self.get_new_books(page=1)
        if new_books:
            # Hydrate with details (limited for testing)
            books_to_hydrate = new_books[:max_books]
            hydrated_books = self.hydrate_book_details(books_to_hydrate)
            
            # Convert to RawBookData
            for book in hydrated_books:
                raw_book = RawBookData(
                    source=SourceType.IT_BOOKSTORE,
                    source_url=f"{self.BASE_URL}books/{book.get('isbn13')}",
                    raw_content=str(book),  # JSON as string
                    http_status=200,
                    ingested_at=datetime.now()
                )
                raw_books.append(raw_book)
        
        return raw_books

# Utility function for easy testing
def get_itbookstore_sample(max_books: int = 5) -> List[RawBookData]:
    """Get a sample of books from IT Bookstore API"""
    api = ITBookstoreAPI()
    return api.get_sample_books(max_books)
    
class ITBookstoreParser:
    """Parser for IT Bookstore API responses"""
    
    @staticmethod
    def parse_book_data(book_json: str) -> Optional[Dict]:
        """Parse book data from JSON response"""
        try:
            import json
            book_data = json.loads(book_json) if isinstance(book_json, str) else book_json
            
            return {
                'title': book_data.get('title', ''),
                'authors': ITBookstoreParser._extract_authors(book_data),
                'price_current': ITBookstoreParser._extract_price(book_data),
                'price_original': ITBookstoreParser._extract_original_price(book_data),
                'currency': 'USD',  # API returns prices in USD
                'availability': 'in_stock',  # Assume in stock for API
                'rating': None,  # Not provided by this API
                'num_reviews': None,  # Not provided
                'description': book_data.get('desc', ''),
                'category': ITBookstoreParser._extract_category(book_data),
                'product_info': ITBookstoreParser._extract_product_info(book_data),
                'upc': book_data.get('isbn13', ''),
                'image_url': book_data.get('image', ''),
                'source_url': book_data.get('url', ''),
            }
            
        except Exception as e:
            logger.error(f"Error parsing IT Bookstore data: {e}")
            return None
    
    @staticmethod
    def _extract_authors(book_data: Dict) -> List[str]:
        """Extract authors from book data"""
        authors = book_data.get('authors', '')
        if authors:
            return [author.strip() for author in authors.split(',')]
        return []
    
    @staticmethod
    def _extract_price(book_data: Dict) -> Optional[float]:
        """Extract current price"""
        price_str = book_data.get('price', '').replace('$', '')
        try:
            return float(price_str) if price_str else None
        except ValueError:
            return None
    
    @staticmethod
    def _extract_original_price(book_data: Dict) -> Optional[float]:
        """Extract original price if available"""
        # This API doesn't provide original prices, but we'll implement for consistency
        return None
    
    @staticmethod
    def _extract_category(book_data: Dict) -> str:
        """Extract category information"""
        # IT Bookstore focuses on technology books
        return book_data.get('subtitle', 'Technology') or 'Technology'
    
    @staticmethod
    def _extract_product_info(book_data: Dict) -> Dict:
        """Extract product information as key-value pairs"""
        info = {
            'isbn10': book_data.get('isbn10', ''),
            'isbn13': book_data.get('isbn13', ''),
            'publisher': book_data.get('publisher', ''),
            'year': book_data.get('year', ''),
            'pages': book_data.get('pages', ''),
            'language': book_data.get('language', ''),
        }
        # Remove empty values
        return {k: v for k, v in info.items() if v}