# Book Data Pipeline - Technical Assessment

## Project Overview

A production-ready data pipeline that ingests book data from multiple web sources, processes it through a medallion architecture, and delivers curated data suitable for analysis. 
Built with modern data engineering tools and best practices.

## Architecture
### Medallion Architecture Implementation

<img width="534" height="329" alt="image" src="https://github.com/user-attachments/assets/acc4ff84-991e-4096-ba70-cbbdda0e84bf" />

## Technology Stack

| Component          |Technology                 | Purpose                          |
|--------------------|---------------------------|----------------------------------|
| Orchestration      | Apache Airflow 2.7        | Pipeline scheduling & monitoring |
| Processing         | PySpark 3.4   | Value C   | Distributed data transformations |
| Storage            | Parquet + Local FS        | Columnar storage for analytics   |
| Containerization   | Docker + Docker Compose   | Environment consistency          |
| Web Scraping       | BeautifulSoup4 + Requests | HTML parsing & HTTP clients      |
| API Integration    | Requests                  | REST API consumption             |

