#!/usr/bin/env python3
"""
Test the complete PySpark pipeline: Scrape → Parse → Spark Transform → Save
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

from ingestion.books_toscrape import scrape_and_parse_sample
from processing.spark_transformer import SparkBookTransformer
from processing.spark_storage import SparkSilverLayerStorage

# Import PySpark functions
from pyspark.sql.functions import mean, min, max, count

def preprocess_data(parsed_data):
    """Preprocess data to avoid enum issues"""
    processed_data = []
    for book in parsed_data:
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
    
    return processed_data

def test_spark_pipeline():
    """Test the complete data pipeline with PySpark"""
    print("🚀 Testing Complete PySpark Pipeline")
    print("=" * 60)
    
    transformer = None
    storage = None
    
    try:
        # Step 1: Scrape and parse
        print("1. 📚 Scraping and parsing books...")
        parsed_books = scrape_and_parse_sample(max_categories=1, max_pages=1)
        print(f"   ✅ Parsed {len(parsed_books)} books")
        
        if not parsed_books:
            print("❌ No books parsed, stopping test")
            return
        
        # Step 2: Preprocess data
        print("2. 🔄 Preprocessing data...")
        processed_data = preprocess_data(parsed_books)
        
        # Step 3: Initialize Spark components
        print("3. 🔧 Initializing Spark...")
        transformer = SparkBookTransformer()
        storage = SparkSilverLayerStorage()
        
        # Step 4: Transform with Spark
        print("4. ⚡ Transforming with PySpark...")
        clean_df = transformer.transform_books_toscrape(processed_data)
        
        # Validate schema
        if transformer.validate_schema(clean_df):
            print("   ✅ Schema validation passed")
        else:
            print("   ❌ Schema validation failed")
            return
        
        # Show DataFrame info
        print(f"   📊 DataFrame: {clean_df.count()} rows, {len(clean_df.columns)} columns")
        
        # Show a cleaner view of the data
        print("   Sample data (clean view):")
        clean_df.select("title", "price_current", "currency", "availability", "data_quality_score").show(3, truncate=50)
        
        # Step 5: Save to silver layer
        print("5. 💾 Saving to silver layer with Spark...")
        saved_path = storage.save_clean_books(
            clean_df, 
            source="books_toscrape", 
            batch_id="test"
        )
        
        # Step 6: Read back and verify
        print("6. 📖 Reading back saved data...")
        read_df = storage.read_clean_books("books_toscrape")
        if read_df:
            print(f"   ✅ Read back {read_df.count()} records")
            print("   Sample of saved data:")
            read_df.select("title", "price_current", "currency", "availability", "data_quality_score").show(3, truncate=50)
        
        # Step 7: Display quality metrics
        print("7. 📈 Data Quality Metrics:")
        quality_stats = clean_df.select(
            mean("data_quality_score").alias("avg_quality"),
            min("data_quality_score").alias("min_quality"),
            max("data_quality_score").alias("max_quality"),
            count("title").alias("total_books")
        ).collect()[0]
        
        print(f"   📊 Average Quality Score: {quality_stats['avg_quality']:.3f}")
        print(f"   📉 Min Quality Score: {quality_stats['min_quality']:.3f}")
        print(f"   📈 Max Quality Score: {quality_stats['max_quality']:.3f}")
        print(f"   📚 Total Books: {quality_stats['total_books']}")
        
        # Step 8: Show schema information
        print("8. 🏗️  Final Schema:")
        clean_df.printSchema()
        
        print("\n🎉 PySpark Pipeline Test COMPLETED!")
        
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up Spark sessions
        if transformer:
            transformer.stop_spark()
        if storage:
            storage.stop_spark()

if __name__ == "__main__":
    test_spark_pipeline()