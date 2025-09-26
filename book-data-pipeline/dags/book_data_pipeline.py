from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def scrape_books_toscrape():
    """Scrape Books to Scrape website with detailed logging"""
    from pyspark.sql import SparkSession
    spark = None
    try:
        print("=== STARTING BOOKS TO SCRAPE TASK ===")
        
        from ingestion.books_toscrape import scrape_and_parse_sample
        from processing.spark_transformer import SparkBookTransformer
        from processing.spark_storage import SparkSilverLayerStorage
        
        print("1. Importing modules...")
        
        # Scrape data
        print("2. Starting web scraping...")
        parsed_books = scrape_and_parse_sample(max_categories=1, max_pages=1)
        print(f"3. Scraped {len(parsed_books)} raw pages")
        
        if not parsed_books:
            print("No books were scraped!")
            return "Failed: No books scraped"
        
        # Create Spark session
        print("4. Creating Spark session...")
        spark = SparkSession.builder.appName("BooksToScrapeIngestion").getOrCreate()
        transformer = SparkBookTransformer()
        
        # Preprocess data
        print("5. Preprocessing data...")
        processed_data = []
        for i, book in enumerate(parsed_books[:3]):  # Just process first 3 for testing
            print(f"   Processing book {i+1}: {book.get('title', 'No title')[:50]}...")
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
        
        print(f"6. Processed {len(processed_data)} books")
        
        # Transform to Spark DataFrame
        print("7. Transforming to Spark DataFrame...")
        clean_df = transformer.transform_books_toscrape(processed_data)
        book_count = clean_df.count()
        print(f"8. Transformed {book_count} books")
        
        if book_count == 0:
            print("DataFrame is empty!")
            return "Failed: Empty DataFrame"
        
        # Show schema and sample
        print("9. DataFrame schema:")
        clean_df.printSchema()
        print("10. Sample data:")
        clean_df.show(2, truncate=50)
        
        # Save to Silver layer
        print("11. Saving to Silver layer...")
        storage = SparkSilverLayerStorage()
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/opt/airflow/storage/silver/books_toscrape/batch_{batch_id}"
        
        print(f"12. Output path: {output_path}")
        storage.save_clean_books(clean_df, "books_toscrape", batch_id)
        
        # Verify files were created
        import time
        time.sleep(2)  # Wait for write to complete
        
        if os.path.exists(output_path):
            files = os.listdir(output_path)
            print(f"SUCCESS: Created {len(files)} files in {output_path}")
            for f in files[:3]:  # Show first 3 files
                print(f"   - {f}")
        else:
            print(f"FAILED: Output path does not exist: {output_path}")
        
        return f"Books to Scrape: {book_count} books processed successfully"
        
    except Exception as e:
        print(f"TASK FAILED: {e}")
        import traceback
        traceback.print_exc()
        return f"Books to Scrape failed: {str(e)}"
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

def scrape_it_bookstore():
    """Scrape IT Bookstore API with detailed logging"""
    from pyspark.sql import SparkSession
    spark = None
    try:
        print("=== STARTING IT BOOKSTORE TASK ===")
        
        from ingestion.itbookstore_api import get_itbookstore_sample
        from processing.spark_transformer import SparkBookTransformer
        from processing.spark_storage import SparkSilverLayerStorage
        
        print("1. Importing modules...")
        
        # Get data from API
        print("2. Calling API...")
        raw_books = get_itbookstore_sample(max_books=3)
        print(f"3. Retrieved {len(raw_books)} raw books from API")
        
        if not raw_books:
            print("No books from API!")
            return "Failed: No API books"
        
        # Create Spark session
        print("4. Creating Spark session...")
        spark = SparkSession.builder.appName("ITBookstoreIngestion").getOrCreate()
        transformer = SparkBookTransformer()
        
        # Preprocess data
        print("5. Preprocessing data...")
        processed_data = []
        for i, book in enumerate(raw_books):
            # Use model_dump() for Pydantic v2
            processed_book = book.model_dump() if hasattr(book, 'model_dump') else book.dict()
            print(f"   Processing book {i+1}: {processed_book.get('title', 'No title')[:50]}...")
            # Convert enums to strings
            if hasattr(processed_book.get('currency'), 'value'):
                processed_book['currency'] = processed_book['currency'].value
            processed_data.append(processed_book)
        
        print(f"6. Processed {len(processed_data)} books")
        
        # Transform to Spark DataFrame
        print("7. Transforming to Spark DataFrame...")
        clean_df = transformer.transform_books_toscrape(processed_data)
        book_count = clean_df.count()
        print(f"8. Transformed {book_count} books")
        
        if book_count == 0:
            print("DataFrame is empty!")
            return "Failed: Empty DataFrame"
        
        # Show sample
        print("9. Sample data:")
        clean_df.show(2, truncate=50)
        
        # Save to Silver layer
        print("10. Saving to Silver layer...")
        storage = SparkSilverLayerStorage()
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/opt/airflow/storage/silver/it_bookstore/batch_{batch_id}"
        
        print(f"11. Output path: {output_path}")
        storage.save_clean_books(clean_df, "it_bookstore", batch_id)
        
        # Verify files were created
        import time
        time.sleep(2)
        
        if os.path.exists(output_path):
            files = os.listdir(output_path)
            print(f"SUCCESS: Created {len(files)} files in {output_path}")
        else:
            print(f"FAILED: Output path does not exist: {output_path}")
        
        return f"IT Bookstore: {book_count} books processed successfully"
        
    except Exception as e:
        print(f"TASK FAILED: {e}")
        import traceback
        traceback.print_exc()
        return f"IT Bookstore failed: {str(e)}"
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

# Keep the other functions similar but add detailed logging
def build_gold_layer():
    """Build Gold layer from Silver data with proper debugging"""
    from pyspark.sql import SparkSession
    spark = None
    try:
        print("=== STARTING GOLD LAYER TASK ===")
        
        from processing.matching import BookMatcher
        from processing.gold_layer import GoldLayerBuilder
        from processing.spark_storage import SparkSilverLayerStorage
        import os
        
        print("1. Creating Spark session...")
        spark = SparkSession.builder.appName("GoldLayer").getOrCreate()
        storage = SparkSilverLayerStorage()
        
        print("2. Checking Silver layer directories...")
        
        # Check what actually exists in silver layer
        silver_paths = [
            "/opt/airflow/storage/silver/books_toscrape",
            "/opt/airflow/storage/silver/it_bookstore"
        ]
        
        for path in silver_paths:
            exists = os.path.exists(path)
            print(f"   {path}: {'EXISTS' if exists else 'MISSING'}")
            if exists:
                batches = os.listdir(path)
                print(f"     Batches found: {batches}")
                for batch in batches:
                    batch_path = os.path.join(path, batch)
                    parquet_files = [f for f in os.listdir(batch_path) if f.endswith('.parquet')]
                    print(f"       Batch {batch}: {len(parquet_files)} parquet files")
        
        print("3. Attempting to read Silver data...")
        books_toscrape = storage.read_clean_books("books_toscrape")
        it_bookstore = storage.read_clean_books("it_bookstore")
        
        print(f"4. books_toscrape DataFrame: {'LOADED' if books_toscrape else '❌ NONE'}")
        print(f"5. it_bookstore DataFrame: {'LOADED' if it_bookstore else '❌ NONE'}")
        
        if books_toscrape is None:
            print("No Books to Scrape data could be loaded")
            # Let's try to read directly to see what's wrong
            try:
                direct_path = "/opt/airflow/storage/silver/books_toscrape/*/*.parquet"
                print(f"   Trying direct read from: {direct_path}")
                test_df = spark.read.parquet(direct_path)
                print(f"   Direct read successful: {test_df.count()} rows")
                books_toscrape = test_df
            except Exception as e:
                print(f"   Direct read also failed: {e}")
                return "Failed to read Books to Scrape data"
        
        if it_bookstore is None:
            print("No IT Bookstore data could be loaded")
            try:
                direct_path = "/opt/airflow/storage/silver/it_bookstore/*/*.parquet"
                print(f"   Trying direct read from: {direct_path}")
                test_df = spark.read.parquet(direct_path)
                print(f"   Direct read successful: {test_df.count()} rows")
                it_bookstore = test_df
            except Exception as e:
                print(f"   Direct read also failed: {e}")
                return "Failed to read IT Bookstore data"
        
        if books_toscrape is None or it_bookstore is None:
            return "Cannot proceed - missing Silver data"
        
        print(f"6. Silver data loaded: {books_toscrape.count()} + {it_bookstore.count()} records")
        
        # Show sample data
        print("7. Books to Scrape sample:")
        books_toscrape.show(2, truncate=30)
        print("8. IT Bookstore sample:")
        it_bookstore.show(2, truncate=30)
        
        # Match books between sources
        print("9. Performing cross-source matching...")
        matcher = BookMatcher(spark)
        matches = matcher.match_books(books_toscrape, it_bookstore)
        match_count = matches.count()
        print(f"10. Found {match_count} cross-source matches")
        
        if match_count > 0:
            print("11. Match examples:")
            matches.select("source1_title", "source2_title", "confidence_score").show(3, truncate=30)
        
        # Combine data
        print("12. Combining data from both sources...")
        all_books = books_toscrape.unionByName(it_bookstore)
        print(f"13. Combined data: {all_books.count()} total records")
        
        # Build Gold layer
        print("14. Building Gold layer...")
        gold_builder = GoldLayerBuilder(spark)
        canonical_books = gold_builder.build_canonical_books(all_books, matches)
        price_history = gold_builder.build_price_history(all_books, matches)
        
        canonical_count = canonical_books.count()
        price_count = price_history.count()
        
        print(f"15. Gold layer created: {canonical_count} canonical books, {price_count} price events")
        
        # Show gold data samples
        print("16. Canonical books sample:")
        canonical_books.show(3, truncate=40)
        print("17. Price history sample:")
        price_history.show(3, truncate=40)
        
        # Save Gold layer
        print("18. Saving Gold layer...")
        gold_storage = SparkSilverLayerStorage("/opt/airflow/storage/gold")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        gold_storage.save_clean_books(canonical_books, "canonical_books", batch_id)
        gold_storage.save_clean_books(price_history, "price_history", batch_id)
        
        # Verify files were created
        gold_paths = [
            f"/opt/airflow/storage/gold/canonical_books/batch_{batch_id}",
            f"/opt/airflow/storage/gold/price_history/batch_{batch_id}"
        ]
        
        for path in gold_paths:
            if os.path.exists(path):
                files = os.listdir(path)
                print(f"GOLD LAYER SUCCESS: {len(files)} files in {path}")
            else:
                print(f"GOLD LAYER FAILED: {path} not created")
        
        return f"Gold layer built: {canonical_count} canonical books, {price_count} price events"
        
    except Exception as e:
        print(f"GOLD LAYER FAILED: {e}")
        import traceback
        traceback.print_exc()
        return f"Gold layer failed: {str(e)}"
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

# Define the DAG (same as before)
with DAG(
    dag_id='book_data_pipeline',
    default_args=default_args,
    description='End-to-end book data pipeline from multiple sources',
    schedule_interval=timedelta(hours=12),
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
        python_callable=lambda: "Report generated",  # Simplified for now
    )
    
    end = DummyOperator(task_id='end_pipeline')
    
    start >> [scrape_books_task, scrape_api_task] >> build_gold_task >> generate_report_task >> end