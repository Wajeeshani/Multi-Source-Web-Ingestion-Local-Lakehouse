# Book Data Pipeline - Technical Assessment

## Project Overview

A production-ready data pipeline that ingests book data from multiple web sources, processes it through a medallion architecture, and delivers curated data suitable for analysis. 
Built with modern data engineering tools and best practices.

## Architecture

### Entity Relationship Diagram

<img width="782" height="625" alt="image" src="https://github.com/user-attachments/assets/32c493c2-1590-4afb-8029-617ae4495524" />

### Medallion Architecture Implementation

<img width="534" height="329" alt="image" src="https://github.com/user-attachments/assets/acc4ff84-991e-4096-ba70-cbbdda0e84bf" />

## Data Dictionary

### Gold Layer - Curated Data Models

1. DIM_BOOK (Canonical Book Dimension) - SCD Type 2
Purpose: Master book entity with historical tracking

| Field              | Type           | Description                            | Constraints             | Example                                   |
|--------------------|----------------|----------------------------------------|-------------------------|-------------------------------------------|
| book_id            | STRING (PK)    | System-generated unique identifier     | Not null, Unique        | bk_001_a22124811bfa8350                   |
| isbn13             | STRING         | International Standard Book Number     | 13 digits, Optional     | 9781912047451                             |
| title              | STRING         | Book title                             | Not null, Max 500 chars | "It's Only the Himalayas"                 |
| authors            | ARRAY<STRING>  | List of authors                        | Array of strings        | ["Simon Long"]                            |
| primary_category   | STRING         | Main category classification           | Not null                | "Travel"                                  |
| category_hierarchy | ARRAY<STRING>  | Full category path                     | Array hierarchy         | ["Books", "Travel", "Adventure"]          |
| valid_from         | TIMESTAMP      | Version start timestamp                | Not null                | 2023-09-24 10:00:00                       |
| valid_to           | TIMESTAMP      | Version end timestamp                  | Null for current        | NULL                                      |
| is_current         | BOOLEAN        | Flag for current version               | Not null                | true                                      |
| confidence_score   | FLOAT          | Matching confidence (0.0-1.0)          | Range 0.0-1.0           | 0.95                                      |
| source_count       | INTEGER        | Number of source systems               | Min 1                   | 2                                         |

2. FACT_PRICE_HISTORY (Price Time Series)
Purpose: Track price changes over time

| Field              | Type        | Description                | Constraints            | Example                       |
|--------------------|-------------|----------------------------|------------------------|-------------------------------|
| book_id            | STRING (FK) | Reference to DIM_BOOK       | Not null               | bk_001_a22124811bfa8350       |
| source             | STRING      | Price source system         | Not null               | "books_toscrape"              |
| price              | FLOAT       | Current price in currency   | ≥ 0.0                  | 45.17                         |
| currency           | STRING      | Currency code               | ISO 4217               | "GBP"                         |
| effective_date     | TIMESTAMP   | When price was effective    | Not null               | 2023-09-24 10:05:00           |
| is_discount        | BOOLEAN     | Discount indicator          | Default false          | false                         |
| discount_percentage| FLOAT       | Discount percentage         | 0.0–100.0, Optional    | 15.5                          |

3. FACT_AVAILABILITY (Availability Timeline)
Purpose: Track stock status changes

| Field           | Type        | Description                   | Constraints   | Example                       |
|-----------------|-------------|-------------------------------|---------------|-------------------------------|
| book_id         | STRING (FK) | Reference to DIM_BOOK         | Not null      | bk_001_a22124811bfa8350       |
| source          | STRING      | Availability source           | Not null      | "it_bookstore"                |
| status          | STRING      | Current availability status   | Enum values   | "in_stock"                    |
| effective_date  | TIMESTAMP   | When status was recorded      | Not null      | 2023-09-24 10:10:00           |
| previous_status | STRING      | Previous status               | Optional      | "out_of_stock"                |


## Technology Stack

| Component          |Technology                 | Purpose                          |
|--------------------|---------------------------|----------------------------------|
| Orchestration      | Apache Airflow 2.7        | Pipeline scheduling & monitoring |
| Processing         | PySpark 3.4   | Value C   | Distributed data transformations |
| Storage            | Parquet + Local FS        | Columnar storage for analytics   |
| Containerization   | Docker + Docker Compose   | Environment consistency          |
| Web Scraping       | BeautifulSoup4 + Requests | HTML parsing & HTTP clients      |
| API Integration    | Requests                  | REST API consumption             |

## Data Sources

1. Books to Scrape (http://books.toscrape.com/)
- Type: HTML Web Scraping
- Volume: ~1,000 books across 50 categories
- Key Features: Pagination handling, rate limiting, error recovery
- Data Extracted: Title, price, availability, ratings, categories, descriptions

2. IT Bookstore API (https://api.itbook.store/1.0/)
- Type: REST API Integration
- Volume: Technology-focused books with detailed metadata
- Key Features: API hydration, structured JSON parsing
- Data Extracted: Title, authors, prices, descriptions, technical details

## Pipeline Components

### Ingestion Layer (src/ingestion/)

- Books to Scrape - HTML Scraper
```
BooksToScrapeScraper()
 ├── get_all_categories()
 ├── scrape_category()
 └── parse_book_page()
```
- IT Bookstore - API Client
```
ITBookstoreAPI()
 ├── get_new_books()
 ├── search_books()
 └── get_book_details()
```
### Processing Layer (src/processing/)
```
# Data Transformation
SparkBookTransformer()
├── transform_books_toscrape()
└── validate_schema()

# Cross-source Matching
BookMatcher()
├── match_books()
├── calculate_matching_scores()
└── select_best_matches()

# Gold Layer Builder
GoldLayerBuilder()
├── build_canonical_books()  # SCD Type 2
└── build_price_history()    # Time series
```
### Data Models (src/utils/schemas.py)

- RawBookData: Bronze layer schema
- CleanBookData: Silver layer schema
- CuratedBookDimension: Gold layer SCD Type 2
- PriceFact: Time-series fact table
- DataQualityResult: Quality monitoring

## Prerequisites

- Docker & Docker Compose
- 4GB+ RAM available for containers

### 1. Clone and Setup
```
git clone https://github.com/Wajeeshani/Multi-Source-Web-Ingestion-Local-Lakehouse.git
cd book-data-pipeline
```
### 2. Start the Environment
```
docker-compose up -d
```
### 3. Access Airflow UI

- URL: http://localhost:8080
- Username: admin
- Password: admin

### 4. Run the Pipeline

- Open Airflow UI → DAGs → book_data_pipeline
- Click Trigger DAG (play button)
- Monitor execution in the Graph View

### 5. Check Results

```
# View processed data
docker-compose exec airflow find /opt/airflow/storage -name "*.parquet" -o -name "*.json"

# Check data quality reports
docker-compose exec airflow cat /opt/airflow/storage/dq_report.json
```
## Data Flow & Processing

### Bronze Layer (Raw)

- Format: Raw HTML/JSON + metadata
- Location: storage/bronze/
- Content: Original source data with ingestion timestamps

### Silver Layer (Cleaned)

- Format: Parquet with enforced schema
- Location: storage/silver/
- Processing:
  - Data validation & cleaning
  - Standardized formats
  - Quality scoring (0.0-1.0)
  - Deduplication

### Gold Layer (Curated)

- Format: Parquet with SCD Type 2
- Location: storage/gold/
- Components:
  - canonical_books/: Master book dimension with history
  - price_history/: Time-series price tracking
  - Cross-source matching with confidence scores

## Key Features

### Cross-Source Matching

```
# Matching Logic
1. Exact ISBN13 match → Confidence: 1.0
2. Fuzzy title similarity (>85%) → Confidence: 0.7-0.9  
3. Author list overlap → Confidence: 0.3-0.6
4. Combined score threshold: 0.6
```
### Data Quality Framework

- Schema validation with Pydantic
- Business rules (positive prices, valid currencies)
- Completeness checks (required fields)
- Quality scoring per record

### Incremental Processing Ready

- Content hashing for change detection
- Idempotent transformations
- SCD Type 2 for historical tracking

### Production Readiness

- Error handling and retry logic
- Rate limiting and polite crawling
- Comprehensive logging
- Containerized deployment

## Project Structure

```
book-data-pipeline/
├── dags/                          # Airflow orchestration
│   └── book_data_pipeline.py     # Main data pipeline
├── src/                          # Source code
│   ├── ingestion/                # Data collection
│   │   ├── books_toscrape.py    # HTML scraper
│   │   └── itbookstore_api.py   # API client
│   ├── processing/               # Data transformation
│   │   ├── spark_transformer.py # PySpark processing
│   │   ├── matching.py          # Cross-source matching
│   │   └── gold_layer.py        # Curated layer builder
│   └── utils/                    # Shared utilities
│       ├── schemas.py           # Data models
│       ├── config.py            # Configuration
│       └── constants.py         # Constants & enums
├── storage/                      # Data lakehouse
│   ├── bronze/                  # Raw data (JSON/HTML)
│   ├── silver/                  # Cleaned data (Parquet)
│   └── gold/                    # Curated data (Parquet)
├── tests/                        # Test suites
├── scripts/                      # Utility scripts
├── docker-compose.yml            # Local deployment
├── Dockerfile                    # Container setup
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```
## Configuration

### Environment Variables

```
# src/utils/config.py
request_delay: float = 1.0          # Seconds between requests
title_similarity_threshold: float = 0.8    # Fuzzy matching threshold
min_price: float = 0.0              # Data validation rules
```

### Airflow Configuration

- Executor: LocalExecutor
- Database: PostgreSQL
- DAG Schedule: 12-hour intervals
- Retry Policy: 1 retry with 5-minute delay

## Testing

### Run Test Suite

```
# Unit tests
docker-compose exec airflow python -m pytest tests/ -v

# Integration test
docker-compose exec airflow python scripts/test_gold_final.py

# End-to-end test
docker-compose exec airflow python scripts/test_spark_pipeline.py
```

### Test Coverage

- Data model validation
- Web scraping components
- API integration
- Spark transformations
- Cross-source matching
- End-to-end pipeline

## Troubleshooting

### Common Issues

- Port conflicts: Change Airflow port in docker-compose.yml
- Memory issues: Increase Docker Desktop resources
- Network errors: Check corporate firewall for API access

### Debugging Commands

```
# Check container status
docker-compose ps

# View logs
docker-compose logs airflow

# Test individual components
docker-compose exec airflow python scripts/test_simple_config.py
```
### Performance Characteristics

- Processing Time: ~3-5 minutes for full pipeline
- Data Volume: Handles 1,000+ books per run
- Memory Usage: ~2GB peak (Spark + Airflow)
- Storage: ~50-100MB per full ingestion

