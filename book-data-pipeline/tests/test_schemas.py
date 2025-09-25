import pytest
from datetime import datetime
from src.utils.schemas import *
from src.utils.constants import *

class TestRawBookData:
    """Test Raw Zone data models"""
    
    def test_raw_book_data_creation(self):
        """Test basic raw book data creation"""
        test_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_url": "https://books.toscrape.com/catalogue/book1.html",
            "raw_content": "<html><title>Test Book</title></html>",
            "http_status": 200,
            "ingested_at": datetime(2023, 1, 1, 10, 0, 0)
        }
        
        raw_book = RawBookData(**test_data)
        
        assert raw_book.source == SourceType.BOOKS_TO_SCRAPE
        assert raw_book.http_status == 200
        assert "Test Book" in raw_book.raw_content
        assert raw_book.generate_content_hash() is not None
    
    def test_raw_book_invalid_url(self):
        """Test URL validation"""
        invalid_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_url": "invalid-url",
            "raw_content": "test content",
            "http_status": 200,
            "ingested_at": datetime.now()
        }
        
        with pytest.raises(ValueError):
            RawBookData(**invalid_data)

class TestCleanBookData:
    """Test Clean Zone data models"""
    
    def test_clean_book_data_creation(self):
        """Test basic clean book data creation"""
        test_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_id": "book_12345",
            "title": "The Great Gatsby",
            "authors": ["F. Scott Fitzgerald"],
            "category_path": "Fiction > Classics",
            "price_current": 12.99,
            "currency": CurrencyType.USD,
            "availability": AvailabilityStatus.IN_STOCK,
            "product_url": "https://books.toscrape.com/catalogue/great-gatsby.html",
            "ingested_at": datetime(2023, 1, 1, 10, 0, 0),
            "content_hash": "abc123def456"
        }
        
        clean_book = CleanBookData(**test_data)
        
        assert clean_book.title == "The Great Gatsby"
        assert clean_book.authors == ["F. Scott Fitzgerald"]
        assert clean_book.price_current == 12.99
        assert clean_book.currency == CurrencyType.USD
        assert clean_book.calculate_discount() is None  # No original price
    
    def test_clean_book_with_discount(self):
        """Test discount calculation"""
        test_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_id": "book_123",
            "title": "Test Book",
            "authors": ["Test Author"],
            "category_path": "Fiction",
            "price_current": 20.00,
            "price_original": 25.00,
            "currency": CurrencyType.USD,
            "availability": AvailabilityStatus.IN_STOCK,
            "product_url": "http://test.com",
            "ingested_at": datetime.now(),
            "content_hash": "test123"
        }
        
        clean_book = CleanBookData(**test_data)
        discount = clean_book.calculate_discount()
        
        assert discount == 20.0  # (25-20)/25 * 100 = 20%
    
    def test_clean_book_authors_cleaning(self):
        """Test author list cleaning"""
        test_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_id": "book_123",
            "title": "Test Book",
            "authors": ["  Author One  ", "", "Author Two  "],
            "category_path": "Fiction",
            "price_current": 10.00,
            "currency": CurrencyType.USD,
            "availability": AvailabilityStatus.IN_STOCK,
            "product_url": "http://test.com",
            "ingested_at": datetime.now(),
            "content_hash": "test123"
        }
        
        clean_book = CleanBookData(**test_data)
        assert clean_book.authors == ["Author One", "Author Two"]  # Cleaned and filtered
    
    def test_clean_book_price_validation(self):
        """Test price validation rules"""
        invalid_data = {
            "source": SourceType.BOOKS_TO_SCRAPE,
            "source_id": "book_123",
            "title": "Test Book",
            "authors": ["Author"],
            "category_path": "Fiction",
            "price_current": -5.00,  # Invalid negative price
            "currency": CurrencyType.USD,
            "availability": AvailabilityStatus.IN_STOCK,
            "product_url": "http://test.com",
            "ingested_at": datetime.now(),
            "content_hash": "test123"
        }
        
        with pytest.raises(ValueError):
            CleanBookData(**invalid_data)

class TestCuratedBookDimension:
    """Test Curated Zone data models"""
    
    def test_curated_book_creation(self):
        """Test curated book dimension creation"""
        curated_book = CuratedBookDimension(
            book_id="bk_001",
            isbn13="9781234567890",
            source_ids={
                SourceType.BOOKS_TO_SCRAPE: ["book_123"],
                SourceType.IT_BOOKSTORE: ["it_book_456"]
            },
            title="Data Engineering Fundamentals",
            authors=["Author A", "Author B"],
            primary_category="Technology",
            category_hierarchy=["Technology", "Computer Science", "Data Engineering"],
            valid_from=datetime(2023, 1, 1),
            confidence_score=0.95,
            source_count=2
        )
        
        assert curated_book.book_id == "bk_001"
        assert curated_book.isbn13 == "9781234567890"
        assert curated_book.source_count == 2
        assert curated_book.is_current == True
        assert curated_book.confidence_score == 0.95
    
    def test_curated_book_isbn_validation(self):
        """Test ISBN13 format validation"""
        with pytest.raises(ValueError):
            CuratedBookDimension(
                book_id="bk_001",
                isbn13="invalid-isbn",  # Invalid format
                title="Test Book",
                authors=["Author"],
                primary_category="Fiction",
                category_hierarchy=["Fiction"],
                valid_from=datetime.now()
            )

class TestFactTables:
    """Test fact table models"""
    
    def test_price_fact_creation(self):
        """Test price fact creation"""
        price_fact = PriceFact(
            book_id="bk_001",
            source=SourceType.BOOKS_TO_SCRAPE,
            price=29.99,
            currency=CurrencyType.USD,
            effective_date=datetime(2023, 1, 1, 12, 0, 0),
            is_discount=True,
            discount_percentage=25.0
        )
        
        assert price_fact.price == 29.99
        assert price_fact.is_discount == True
        assert price_fact.discount_percentage == 25.0
    
    def test_availability_fact_creation(self):
        """Test availability fact creation"""
        availability_fact = AvailabilityFact(
            book_id="bk_001",
            source=SourceType.IT_BOOKSTORE,
            status=AvailabilityStatus.OUT_OF_STOCK,
            effective_date=datetime(2023, 1, 1, 14, 0, 0),
            previous_status=AvailabilityStatus.IN_STOCK
        )
        
        assert availability_fact.status == AvailabilityStatus.OUT_OF_STOCK
        assert availability_fact.previous_status == AvailabilityStatus.IN_STOCK

class TestDataQualityModels:
    """Test data quality models"""
    
    def test_data_quality_rule(self):
        """Test data quality rule creation"""
        rule = DataQualityRule(
            rule_name="price_positive",
            rule_type=DQRuleType.RANGE,
            field_name="price_current",
            parameters={"min_value": 0.0, "max_value": 1000.0}
        )
        
        assert rule.rule_name == "price_positive"
        assert rule.rule_type == DQRuleType.RANGE
    
    def test_pipeline_run_report(self):
        """Test pipeline run report creation"""
        report = PipelineRunReport(
            run_id="run_20230101_100000",
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            status="SUCCESS",
            source=SourceType.BOOKS_TO_SCRAPE,
            records_ingested=1000,
            records_cleaned=950,
            records_deduplicated=50,
            records_rejected=50
        )
        
        assert report.run_id == "run_20230101_100000"
        assert report.calculate_success_rate() == 95.0  # 950/1000 * 100

class TestEnumValues:
    """Test enum values and mappings"""
    
    def test_source_type_enum(self):
        """Test source type enum values"""
        assert SourceType.BOOKS_TO_SCRAPE.value == "books_toscrape"
        assert SourceType.IT_BOOKSTORE.value == "it_bookstore"
    
    def test_availability_status_enum(self):
        """Test availability status enum values"""
        assert AvailabilityStatus.IN_STOCK.value == "in_stock"
        assert AvailabilityStatus.OUT_OF_STOCK.value == "out_of_stock"
    
    def test_currency_type_enum(self):
        """Test currency type enum values"""
        assert CurrencyType.USD.value == "USD"
        assert CurrencyType.GBP.value == "GBP"