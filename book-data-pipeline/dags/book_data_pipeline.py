from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def scrape_books_toscrape():
    """Scrape Books to Scrape website"""
    try:
        from ingestion.books_toscrape import scrape_and_parse_sample
        from processing.spark_transformer import SparkBookTransformer
        from processing.spark_storage import SparkSilverLayerStorage
        
        print("📚 Starting Books to Scrape ingestion...")
        
        # Scrape data
        parsed_books = scrape_and_parse_sample(max_categories=1, max_pages=1)
        print(f"✅ Scraped {len(parsed_books)} raw pages")
        
        # Transform with Spark
        transformer = SparkBookTransformer()
        
        # Preprocess data
        processed_data = []
        for book in parsed_books:
            processed_book = book.copy()
            # Convert enums to strings
            if hasattr(processed_book.get('currency'), 'value'):
                processed_book['currency'] = processed_book['currency'].value
            if hasattr(processed_book.get('availability'), 'value'):
                processed_book['availability'] = processed_book['availability'].value
            # Ensure authors is a list
            if not isinstance(processed_book.get('authors'), list):
                processed_book['authors'] = []
            processed_data.append(processed_book)
        
        # Transform to Spark DataFrame
        clean_df = transformer.transform_books_toscrape(processed_data)
        book_count = clean_df.count()
        print(f"✅ Transformed {book_count} books")
        
        # Save to Silver layer
        storage = SparkSilverLayerStorage()
        storage.save_clean_books(clean_df, "books_toscrape", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        transformer.stop_spark()
        
        return f"✅ Books to Scrape: {book_count} books processed"
        
    except Exception as e:
        return f"❌ Books to Scrape failed: {str(e)}"

def scrape_it_bookstore():
    """Scrape IT Bookstore API"""
    try:
        from ingestion.itbookstore_api import get_itbookstore_sample
        from processing.spark_transformer import SparkBookTransformer
        from processing.spark_storage import SparkSilverLayerStorage
        
        print("📚 Starting IT Bookstore API ingestion...")
        
        # Get data from API
        raw_books = get_itbookstore_sample(max_books=5)
        print(f"✅ Retrieved {len(raw_books)} raw books from API")
        
        # Transform with Spark
        transformer = SparkBookTransformer()
        
        # Preprocess data
        processed_data = []
        for book in raw_books:
            processed_book = book.dict()
            # Convert enums to strings
            if hasattr(processed_book.get('currency'), 'value'):
                processed_book['currency'] = processed_book['currency'].value
            if hasattr(processed_book.get('availability'), 'value'):
                processed_book['availability'] = processed_book['availability'].value
            processed_data.append(processed_book)
        
        # Transform to Spark DataFrame
        clean_df = transformer.transform_books_toscrape(processed_data)
        book_count = clean_df.count()
        print(f"✅ Transformed {book_count} books")
        
        # Save to Silver layer
        storage = SparkSilverLayerStorage()
        storage.save_clean_books(clean_df, "it_bookstore", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        transformer.stop_spark()
        
        return f"✅ IT Bookstore: {book_count} books processed"
        
    except Exception as e:
        return f"❌ IT Bookstore failed: {str(e)}"

def build_gold_layer():
    """Build Gold layer from Silver data"""
    try:
        from pyspark.sql import SparkSession
        from processing.matching import BookMatcher
        from processing.gold_layer import GoldLayerBuilder
        from processing.spark_storage import SparkSilverLayerStorage
        
        print("🏗️ Starting Gold layer construction...")
        
        spark = SparkSession.builder.appName("GoldLayer").getOrCreate()
        storage = SparkSilverLayerStorage()
        matcher = BookMatcher(spark)
        gold_builder = GoldLayerBuilder(spark)
        
        # Read Silver data
        books_toscrape = storage.read_clean_books("books_toscrape")
        it_bookstore = storage.read_clean_books("it_bookstore")
        
        if books_toscrape is None:
            return "❌ No Books to Scrape data found in Silver layer"
        if it_bookstore is None:
            return "❌ No IT Bookstore data found in Silver layer"
        
        print(f"📖 Silver layer: {books_toscrape.count()} + {it_bookstore.count()} records")
        
        # Match books between sources
        matches = matcher.match_books(books_toscrape, it_bookstore)
        match_count = matches.count()
        print(f"🔗 Found {match_count} cross-source matches")
        
        if match_count > 0:
            matches.select("source1_title", "source2_title", "confidence_score").show(3, truncate=30)
        
        # Combine data
        all_books = books_toscrape.unionByName(it_bookstore)
        
        # Build Gold layer
        canonical_books = gold_builder.build_canonical_books(all_books, matches)
        price_history = gold_builder.build_price_history(all_books, matches)
        
        canonical_count = canonical_books.count()
        price_count = price_history.count()
        
        print(f"💰 Gold layer: {canonical_count} canonical books, {price_count} price events")
        
        # Save Gold layer
        gold_storage = SparkSilverLayerStorage("/opt/airflow/storage/gold")
        gold_storage.save_clean_books(canonical_books, "canonical_books", datetime.now().strftime("%Y%m%d_%H%M%S"))
        gold_storage.save_clean_books(price_history, "price_history", datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        spark.stop()
        
        return f"✅ Gold layer built: {canonical_count} canonical books, {price_count} price events"
        
    except Exception as e:
        return f"❌ Gold layer failed: {str(e)}"

def generate_report():
    """Generate pipeline report"""
    try:
        import json
        from datetime import datetime
        
        report = {
            "pipeline_run": datetime.now().isoformat(),
            "status": "completed",
            "layers": {
                "bronze": "Raw HTML/JSON from sources",
                "silver": "Cleaned and validated data", 
                "gold": "Curated and matched data"
            },
            "sources": [
                {
                    "name": "Books to Scrape",
                    "type": "Web scraping",
                    "description": "HTML scraping of book catalog"
                },
                {
                    "name": "IT Bookstore API", 
                    "type": "REST API",
                    "description": "JSON API for technology books"
                }
            ],
            "technologies": [
                "Apache Airflow (orchestration)",
                "PySpark (processing)",
                "Parquet (storage)",
                "Docker (containerization)"
            ]
        }
        
        # Save report
        report_path = "/opt/airflow/storage/pipeline_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📊 Pipeline report saved to: {report_path}")
        return "✅ Pipeline report generated"
        
    except Exception as e:
        return f"❌ Report generation failed: {str(e)}"

# Define the DAG
with DAG(
    dag_id='book_data_pipeline',
    default_args=default_args,
    description='End-to-end book data pipeline from multiple sources',
    schedule_interval=timedelta(hours=12),  # Run every 12 hours
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['books', 'data-engineering', 'pyspark'],
) as dag:

    start = DummyOperator(task_id='start_pipeline')
    
    scrape_books_task = PythonOperator(
        task_id='scrape_books_toscrape',
        python_callable=scrape_books_toscrape,
    )
    
    scrape_api_task = PythonOperator(
        task_id='scrape_it_bookstore',
        python_callable=scrape_it_bookstore,
    )
    
    build_gold_task = PythonOperator(
        task_id='build_gold_layer',
        python_callable=build_gold_layer,
    )
    
    generate_report_task = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=generate_report,
    )
    
    end = DummyOperator(task_id='end_pipeline')
    
    # Define workflow: parallel scraping → gold layer → report
    start >> [scrape_books_task, scrape_api_task] >> build_gold_task >> generate_report_task >> end