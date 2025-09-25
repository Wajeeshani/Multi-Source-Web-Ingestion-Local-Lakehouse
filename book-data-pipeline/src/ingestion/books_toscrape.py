import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import time
import logging
from datetime import datetime
from urllib.parse import urljoin
import re

from utils.schemas import RawBookData, SourceType, CleanBookData, AvailabilityStatus, CurrencyType
from utils.config import settings

logger = logging.getLogger(__name__)

class BooksToScrapeScraper:
    """
    Scraper for books.toscrape.com website
    Handles pagination, rate limiting, and error handling
    """

    BASE_URL = "http://books.toscrape.com/"
    DEFAULT_DELAY = 1.0  # seconds between requests

    def __init__(self, delay: float = None):
        self.delay = delay or settings.request_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_all_categories(self) -> List[Dict]:
        """Get all available book categories from the homepage"""
        try:
            response = self.session.get(self.BASE_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            category_links = soup.select('.side_categories ul li ul li a')

            categories = []
            for link in category_links:
                categories.append({
                    'name': link.text.strip(),
                    'url': urljoin(self.BASE_URL, link['href']),
                    'count': self._extract_count_from_url(link['href'])
                })

            logger.info(f"Found {len(categories)} categories")
            return categories

        except requests.RequestException as e:
            logger.error(f"Error fetching categories: {e}")
            return []

    def scrape_category(self, category_url: str, max_pages: int = None) -> List[RawBookData]:
        """Scrape all books from a category page (with pagination)"""
        raw_books = []
        page_url = category_url
        page_count = 0

        while page_url and (max_pages is None or page_count < max_pages):
            try:
                logger.info(f"Scraping page: {page_url}")

                # Fetch page content
                response = self.session.get(page_url)
                response.raise_for_status()

                # Create raw book data entry for category page
                raw_book = RawBookData(
                    source=SourceType.BOOKS_TO_SCRAPE,
                    source_url=page_url,
                    raw_content=response.text,
                    http_status=response.status_code,
                    ingested_at=datetime.now()
                )
                raw_books.append(raw_book)

                # Parse book links from this page
                book_urls = self._extract_book_urls(response.text)
                logger.info(f"Found {len(book_urls)} books on page")

                # Scrape individual book pages
                for book_url in book_urls:
                    book_data = self._scrape_single_book(book_url)
                    if book_data:
                        raw_books.append(book_data)
                    time.sleep(self.delay)  # Be polite

                # Check for next page
                page_url = self._get_next_page_url(response.text, page_url)
                page_count += 1
                time.sleep(self.delay)

            except requests.RequestException as e:
                logger.error(f"Error scraping page {page_url}: {e}")
                break

        return raw_books

    def _scrape_single_book(self, book_url: str) -> Optional[RawBookData]:
        """Scrape a single book detail page"""
        try:
            response = self.session.get(book_url)
            response.raise_for_status()

            return RawBookData(
                source=SourceType.BOOKS_TO_SCRAPE,
                source_url=book_url,
                raw_content=response.text,
                http_status=response.status_code,
                ingested_at=datetime.now()
            )

        except requests.RequestException as e:
            logger.error(f"Error scraping book {book_url}: {e}")
            return None

    def scrape_and_parse_category(self, category_url: str, max_pages: int = 1) -> List[Dict]:
        """Scrape and parse books from a category"""
        parsed_books = []

        raw_pages = self.scrape_category(category_url, max_pages)

        for raw_page in raw_pages:
            if 'catalogue/' in raw_page.source_url:
                book_data = self.parse_book_page(raw_page.raw_content, raw_page.source_url)
                if book_data:
                    book_data['source_url'] = raw_page.source_url
                    book_data['ingested_at'] = raw_page.ingested_at
                    book_data['http_status'] = raw_page.http_status
                    parsed_books.append(book_data)

        logger.info(f"Parsed {len(parsed_books)} books from category")
        return parsed_books

    def parse_book_page(self, html_content: str, source_url: str) -> Optional[Dict]:
        """Parse book details from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            title = self._extract_title(soup)
            price = self._extract_price(soup)

            if not title or not price:
                logger.warning(f"Could not extract title or price from page")
                return None

            availability = self._extract_availability(soup)
            rating = self._extract_rating(soup)
            description = self._extract_description(soup)
            product_info = self._extract_product_info(soup)
            
            # Extract number of reviews from product info
            num_reviews = 0
            reviews_text = product_info.get('number_of_reviews', '0')
            if reviews_text.isdigit():
                num_reviews = int(reviews_text)

            return {
                'title': title,
                'authors': self._extract_authors(soup),
                'price_current': price,
                'price_original': self._extract_original_price(soup, price),
                'currency': self._extract_currency(soup),
                'availability': availability,
                'rating': rating,
                'num_reviews': num_reviews,
                'description': description,
                'category': self._extract_category_from_breadcrumb(soup),
                'product_info': product_info,
                'upc': self._extract_upc(product_info),
                'image_url': self._extract_image_url(soup, source_url),
            }

        except Exception as e:
            logger.error(f"Error parsing book page: {e}")
            return None

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        title_elem = soup.select_one('h1')
        return title_elem.text.strip() if title_elem else None

    def _extract_price(self, soup: BeautifulSoup) -> Optional[float]:
        price_elem = soup.select_one('.price_color')
        if price_elem:
            price_text = price_elem.text.strip()
            try:
                return float(re.sub(r'[^\d.]', '', price_text))
            except ValueError:
                return None
        return None

    def _extract_original_price(self, soup: BeautifulSoup, current_price: float) -> Optional[float]:
        return None

    def _extract_currency(self, soup: BeautifulSoup) -> str:
        price_elem = soup.select_one('.price_color')
        if price_elem:
            price_text = price_elem.text.strip()
            if '£' in price_text:
                return CurrencyType.GBP
            elif '$' in price_text:
                return CurrencyType.USD
            elif '€' in price_text:
                return CurrencyType.EUR
        return CurrencyType.GBP

    def _extract_availability(self, soup: BeautifulSoup) -> str:
        availability_elem = soup.select_one('.availability')
        if availability_elem:
            availability_text = availability_elem.text.strip().lower()
            if 'in stock' in availability_text:
                return AvailabilityStatus.IN_STOCK
            elif 'out of stock' in availability_text:
                return AvailabilityStatus.OUT_OF_STOCK
        return AvailabilityStatus.UNKNOWN

    def _extract_rating(self, soup: BeautifulSoup) -> Optional[float]:
        rating_elem = soup.select_one('.star-rating')
        if rating_elem:
            rating_classes = rating_elem.get('class', [])
            for cls in rating_classes:
                if cls.startswith('One'):
                    return 1.0
                elif cls.startswith('Two'):
                    return 2.0
                elif cls.startswith('Three'):
                    return 3.0
                elif cls.startswith('Four'):
                    return 4.0
                elif cls.startswith('Five'):
                    return 5.0
        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract book description """
        selectors = ['#product_description + p', '.product_page > p']
        for selector in selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                # Fix encoding issues
                text = desc_elem.text.strip()
                # Replace common encoding issues
                text = text.replace('â', "'").replace('Â', '')
                return text
        return None

    def _extract_product_info(self, soup: BeautifulSoup) -> Dict:
        """Extract product information table """
        product_info = {}
        table = soup.find('table', class_='table table-striped')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                header = row.find('th')
                value = row.find('td')
                if header and value:
                    key = header.text.strip().lower().replace(' ', '_')
                    # Fix encoding in values
                    clean_value = value.text.strip().replace('Â', '')
                    product_info[key] = clean_value
        return product_info

    def _extract_upc(self, product_info: Dict) -> Optional[str]:
        return product_info.get('upc', None) or product_info.get('universal_product_code', None)

    def _extract_authors(self, soup: BeautifulSoup) -> List[str]:
        """Extract authors from the product page"""
        # First check if there's a specific author row in the table
        author_th = soup.find('th', string=lambda text: text and 'author' in text.lower())
        if author_th:
            author_td = author_th.find_next_sibling('td')
            if author_td:
                authors_text = author_td.text.strip()
                if authors_text:
                    return [author.strip() for author in authors_text.split(',')]
        
        # If no author row found, check the product info we already extracted
        product_info = self._extract_product_info(soup)
        for key, value in product_info.items():
            if 'author' in key.lower() and value.strip():
                return [author.strip() for author in value.split(',')]
        
        return ['Unknown Author']

    def _extract_category_from_url(self, url: str) -> str:
        parts = url.split('/')
        for i, part in enumerate(parts):
            if part == 'category' and i + 2 < len(parts):
                return parts[i + 2].replace('_', ' ').title()
        return "Unknown"

    def _extract_image_url(self, soup: BeautifulSoup, source_url: str) -> Optional[str]:
        img_elem = soup.select_one('.item.active img')
        if img_elem and img_elem.get('src'):
            return urljoin(source_url, img_elem['src'])
        return None

    def _extract_book_urls(self, html_content: str) -> List[str]:
        """Extract book URLs from a category page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        book_links = soup.select('h3 a')
        
        urls = []
        for link in book_links:
            relative_url = link['href']
            # Correct relative URLs
            # Some are like '../../../book-title_123/index.html'
            # Remove only leading '../' and prepend catalogue path
            clean_url = relative_url.replace('../../../', '')
            book_url = urljoin(self.BASE_URL + 'catalogue/', clean_url)
            urls.append(book_url)
        
        return urls

    def _get_next_page_url(self, html_content: str, current_url: str) -> Optional[str]:
        soup = BeautifulSoup(html_content, 'html.parser')
        next_button = soup.select_one('li.next a')
        if next_button:
            return urljoin(current_url, next_button['href'])
        return None

    def _extract_count_from_url(self, url: str) -> Optional[int]:
        return None

    def _extract_category_from_breadcrumb(self, soup: BeautifulSoup) -> str:
        """Extract category from breadcrumb navigation"""
        breadcrumb = soup.select('.breadcrumb li')
        if len(breadcrumb) >= 3:
            category_elem = breadcrumb[2]
            return category_elem.text.strip()
        return "Unknown"

# Utility function for easy testing
def scrape_and_parse_sample(max_categories: int = 1, max_pages: int = 1) -> List[Dict]:
    """Scrape and parse a sample of books for testing"""
    scraper = BooksToScrapeScraper()
    categories = scraper.get_all_categories()
    if max_categories:
        categories = categories[:max_categories]

    all_parsed_books = []
    for category in categories:
        logger.info(f"Scraping category: {category['name']}")
        parsed_books = scraper.scrape_and_parse_category(
            category['url'],
            max_pages=max_pages
        )
        all_parsed_books.extend(parsed_books)

    logger.info(f"Total parsed books: {len(all_parsed_books)}")
    return all_parsed_books
