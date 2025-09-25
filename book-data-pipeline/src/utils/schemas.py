from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import hashlib
import json

class SourceType(str, Enum):
    BOOKS_TO_SCRAPE = "books_toscrape"
    IT_BOOKSTORE = "it_bookstore"

class AvailabilityStatus(str, Enum):
    IN_STOCK = "in_stock"
    OUT_OF_STOCK = "out_of_stock"
    PRE_ORDER = "pre_order"
    UNKNOWN = "unknown"

class CurrencyType(str, Enum):
    USD = "USD"
    GBP = "GBP"
    EUR = "EUR"
    UNKNOWN = "UNK"

# ===== RAW ZONE SCHEMAS =====
class RawBookData(BaseModel):
    """Raw data as ingested from sources - Bronze Layer"""
    source: SourceType
    source_url: str
    raw_content: str  # HTML or JSON
    http_status: int
    ingested_at: datetime
    file_path: Optional[str] = None  # For local snapshots
    
    @validator('source_url')
    def validate_source_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('source_url must be a valid URL')
        return v
    
    def generate_content_hash(self) -> str:
        """Generate hash for deduplication"""
        content_to_hash = f"{self.source_url}_{self.raw_content[:1000]}"
        return hashlib.md5(content_to_hash.encode()).hexdigest()

# ===== CLEAN ZONE SCHEMAS =====
class CleanBookData(BaseModel):
    """Cleaned and standardized book data - Silver Layer"""
    # Identification
    source: SourceType
    source_id: str = Field(..., description="Unique ID from source system")
    isbn13: Optional[str] = Field(None, pattern=r'^\d{13}$')
    
    # Core metadata
    title: str = Field(..., min_length=1, max_length=500)
    authors: List[str] = Field(default_factory=list)
    category_path: str = Field(..., min_length=1)
    
    # Pricing
    price_current: float = Field(..., ge=0.0, le=10000.0)
    price_original: Optional[float] = Field(None, ge=0.0, le=10000.0)
    currency: CurrencyType = CurrencyType.UNKNOWN
    
    # Availability
    availability: AvailabilityStatus = AvailabilityStatus.UNKNOWN
    stock_quantity: Optional[int] = Field(None, ge=0)
    
    # Ratings and reviews
    rating: Optional[float] = Field(None, ge=0.0, le=5.0)
    num_reviews: Optional[int] = Field(None, ge=0)
    
    # Additional details
    specs: Dict[str, Any] = Field(default_factory=dict)
    image_url: Optional[str] = None
    product_url: str
    description: Optional[str] = None
    
    # Technical metadata
    ingested_at: datetime
    content_hash: str = Field(..., description="Hash for deduplication")
    data_quality_score: float = Field(1.0, ge=0.0, le=1.0)
    
    @validator('price_original')
    def validate_price_original(cls, v, values):
        if v is not None and 'price_current' in values:
            if v < values['price_current']:
                raise ValueError('price_original must be >= price_current')
        return v
    
    @validator('authors')
    def validate_authors(cls, v):
        return [author.strip() for author in v if author.strip()]
    
    def calculate_discount(self) -> Optional[float]:
        """Calculate discount percentage"""
        if self.price_original and self.price_original > 0:
            return ((self.price_original - self.price_current) / self.price_original) * 100
        return None

# ===== CURATED ZONE SCHEMAS =====
class CuratedBookDimension(BaseModel):
    """Canonical book entity with SCD Type 2 - Gold Layer"""
    # Surrogate key
    book_id: str = Field(..., description="System-generated unique ID")
    
    # Natural keys
    isbn13: Optional[str] = Field(None, pattern=r'^\d{13}$')
    source_ids: Dict[SourceType, List[str]] = Field(default_factory=dict)
    
    # Core attributes (slowly changing)
    title: str
    authors: List[str]
    primary_category: str
    category_hierarchy: List[str]
    
    # SCD Type 2 metadata
    valid_from: datetime
    valid_to: Optional[datetime] = None
    is_current: bool = True
    version: int = 1
    
    # Quality metrics
    confidence_score: float = Field(0.0, ge=0.0, le=1.0)
    source_count: int = Field(1, ge=1)

class PriceFact(BaseModel):
    """Price time series fact table"""
    book_id: str
    source: SourceType
    price: float
    currency: CurrencyType
    effective_date: datetime
    is_discount: bool = False
    discount_percentage: Optional[float] = None

class AvailabilityFact(BaseModel):
    """Availability timeline fact table"""
    book_id: str
    source: SourceType
    status: AvailabilityStatus
    effective_date: datetime
    previous_status: Optional[AvailabilityStatus] = None

class CategoryDimension(BaseModel):
    """Category hierarchy dimension"""
    category_id: str
    category_name: str
    parent_category_id: Optional[str] = None
    level: int
    full_path: str

# ===== DATA QUALITY SCHEMAS =====
class DataQualityRule(BaseModel):
    """Data quality rule definition"""
    rule_name: str
    rule_type: str  # not_null, unique, range, regex, custom
    field_name: str
    parameters: Dict[str, Any] = Field(default_factory=dict)

class DataQualityResult(BaseModel):
    """Data quality check result"""
    rule_name: str
    status: str  # PASS, FAIL, WARNING
    field_name: str
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    error_message: Optional[str] = None
    timestamp: datetime

class PipelineRunReport(BaseModel):
    """Run-level metrics and reporting"""
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str  # SUCCESS, FAILED, RUNNING
    source: SourceType
    
    # Metrics
    records_ingested: int = 0
    records_cleaned: int = 0
    records_deduplicated: int = 0
    records_rejected: int = 0
    data_quality_failures: int = 0
    
    # Errors
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    
    def calculate_success_rate(self) -> float:
        if self.records_ingested == 0:
            return 0.0
        return (self.records_cleaned / self.records_ingested) * 100