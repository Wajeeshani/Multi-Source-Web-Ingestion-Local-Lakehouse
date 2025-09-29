# Architecture Overview
A production-ready data pipeline that ingests book data from multiple web sources, processes it through a medallion architecture, and delivers curated data suitable for analysis. 

## 1. Project Structure
This section provides a high-level overview of the project's directory and file structure, categorised by architectural layer or major functional area. It is essential for quickly navigating the codebase, locating relevant files, and understanding the overall organization and separation of concerns.

```
book-data-pipeline/
├── .gitignore
├── README.md
├── ARCHITECTURE.md
├── DATA_DICTIONARY.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
│
├── dags/
│   └── book_data_pipeline.py
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── books_toscrape.py
│   │   ├── itbookstore_api.py
│   │   └── utils.py
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── spark_transformer.py
│   │   ├── spark_storage.py
│   │   ├── matching.py
│   │   └── gold_layer.py
│   └── utils/
│       ├── __init__.py
│       ├── schemas.py
│       ├── config.py
│       └── constants.py
│
├── scripts/
│   ├── __init__.py
│   ├── test_scraper.py
│   ├── test_spark_pipeline.py
│   ├── test_gold_final.py
│   ├── test_itbookstore.py
│   ├── verify_imports.py
│   └── show_matches.py
│
├── tests/
│   ├── __init__.py
│   ├── test_schemas.py
│   ├── test_books_toscrape.py
│   ├── test_itbookstore_api.py
│   └── test_processing.py
│
├── storage/
│   ├── bronze/
│   │   └── .gitkeep
│   ├── silver/
│   │   └── .gitkeep
│   └── gold/
│       └── .gitkeep
│
├── docs/
│   ├── images/
│   │   └── architecture.png
│   ├── setup_guide.md
│   └── troubleshooting.md
│
└── examples/
    ├── sample_bronze_data.json
    ├── sample_silver_data.parquet/
    └── sample_gold_data.parquet/
```

## 2. High-Level System Diagram

[Web Sources]
     ↓
[Airflow DAG]
     ↓
[Bronze Layer] - Raw HTML/JSON
     ↓  
[Silver Layer] - Cleaned, validated data (PySpark)
     ↓
[Gold Layer] - Curated, matched, historical data
     ↓
[Analysis Ready] - Parquet files for consumption

## 3. Core Components

| Component          |Technology                 | Purpose                          |
|--------------------|---------------------------|----------------------------------|
| Orchestration      | Apache Airflow 2.7        | Pipeline scheduling & monitoring |
| Processing         | PySpark 3.4   | Value C   | Distributed data transformations |
| Storage            | Parquet + Local FS        | Columnar storage for analytics   |
| Containerization   | Docker + Docker Compose   | Environment consistency          |
| Web Scraping       | BeautifulSoup4 + Requests | HTML parsing & HTTP clients      |
| API Integration    | Requests                  | REST API consumption             |

## 4.  Data Sources

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

## 5. Pipeline Components

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

## 6. storage layer

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
