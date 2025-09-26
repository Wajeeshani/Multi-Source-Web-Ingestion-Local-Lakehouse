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

### Silver Layer - Cleaned Data Models

1. CLEAN_BOOKS (Standardized Book Data)
Purpose: Cleaned and validated book data from all sources

| **Field**          | **Type**           | **Description**                | **Constraints**                        | **Source Systems** |
| ------------------ | ------------------ | ------------------------------ | -------------------------------------- | ------------------ |
| source             | STRING             | Data source identifier         | Enum: `books_toscrape`, `it_bookstore` | Both               |
| source_id          | STRING             | Source system unique ID        | Not null                               | Both               |
| isbn13             | STRING             | ISBN-13 identifier             | 13 digits, Optional                    | Both               |
| title              | STRING             | Book title                     | Not null, Max 500 chars                | Both               |
| authors            | ARRAY<STRING>      | List of authors                | Array, Min 0 items                     | Both               |
| category_path      | STRING             | Full category hierarchy        | Not null                               | Both               |
| price_current      | FLOAT              | Current price                  | 0.0 – 10000.0                          | Both               |
| price_original     | FLOAT              | Original price (if discount)   | ≥ price_current                        | Books to Scrape    |
| currency           | STRING             | Currency code                  | Valid ISO codes                        | Both               |
| availability       | STRING             | Stock status                   | Enum values                            | Both               |
| stock_quantity     | INTEGER            | Available quantity             | ≥ 0, Optional                          | Books to Scrape    |
| rating             | FLOAT              | User rating                    | 0.0 – 5.0, Optional                    | Books to Scrape    |
| num_reviews        | INTEGER            | Review count                   | ≥ 0, Optional                          | Both               |
| specs              | MAP<STRING,STRING> | Additional specifications      | Key-value pairs                        | Both               |
| image_url          | STRING             | Book cover image URL           | URL format, Optional                   | Both               |
| product_url        | STRING             | Product page URL               | URL format, Not null                   | Both               |
| description        | STRING             | Book description               | Optional, Text                         | Both               |
| ingested_at        | TIMESTAMP          | Data ingestion timestamp       | Not null                               | Both               |
| content_hash       | STRING             | Content hash for deduplication | MD5, Not null                          | Both               |
| data_quality_score | FLOAT              | Data quality assessment        | 0.0 – 1.0                              | Both               |


2. DATA_QUALITY_RESULTS (Quality Metrics)
Purpose: Data quality validation results

| **Field**      | **Type**  | **Description**         | **Constraints**       |
| -------------- | --------- | ----------------------- | --------------------- |
| run_id         | STRING    | Pipeline run identifier | Not null              |
| rule_name      | STRING    | Quality rule name       | Not null              |
| status         | STRING    | Validation status       | PASS / FAIL / WARNING |
| field_name     | STRING    | Validated field name    | Optional              |
| expected_value | STRING    | Expected value          | Optional              |
| actual_value   | STRING    | Actual value            | Optional              |
| error_message  | STRING    | Error description       | Optional              |
| timestamp      | TIMESTAMP | Validation timestamp    | Not null              |


### Bronze Layer - Raw Data Models

1. RAW_BOOK_DATA (Source System Data)
Purpose: Raw data as ingested from source systems

| **Field**   | **Type**  | **Description**            | **Source Specific** |
| ----------- | --------- | -------------------------- | ------------------- |
| source      | STRING    | Source system identifier   | Both                |
| source_url  | STRING    | URL where data was fetched | Both                |
| raw_content | STRING    | Raw HTML or JSON content   | Both                |
| http_status | INTEGER   | HTTP response status code  | Both                |
| ingested_at | TIMESTAMP | Ingestion timestamp        | Both                |
| file_path   | STRING    | Local file path (if saved) | Both                |
