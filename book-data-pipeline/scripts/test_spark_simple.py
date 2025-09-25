#!/usr/bin/env python3
"""
Simple test for PySpark transformation
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

def test_spark_simple():
    """Simple test to verify PySpark works"""
    print("🧪 Testing PySpark basic functionality...")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import lit
        
        # Test basic Spark functionality
        spark = SparkSession.builder \
            .appName("TestSpark") \
            .getOrCreate()
        
        # Create a simple DataFrame
        data = [("Book1", 29.99), ("Book2", 39.99)]
        columns = ["title", "price"]
        
        df = spark.createDataFrame(data, columns)
        df.show()
        
        # Test transformations
        df_transformed = df.withColumn("currency", lit("USD"))
        df_transformed.show()
        
        print(f"✅ Spark test successful! Row count: {df.count()}")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_preparation():
    """Test data preparation without Spark"""
    print("\n🧪 Testing data preparation...")
    
    try:
        from ingestion.books_toscrape import scrape_and_parse_sample
        
        # Get a small sample of data
        parsed_books = scrape_and_parse_sample(max_categories=1, max_pages=1)
        print(f"✅ Got {len(parsed_books)} parsed books")
        
        # Check the structure of the first book
        if parsed_books:
            sample = parsed_books[0]
            print("Sample book keys:", list(sample.keys()))
            print("Title:", sample.get('title'))
            print("Price:", sample.get('price_current'))
            print("Currency type:", type(sample.get('currency')))
            
            # Fix enum issue by converting to string
            if hasattr(sample.get('currency'), 'value'):
                sample['currency'] = sample['currency'].value
            if hasattr(sample.get('availability'), 'value'):
                sample['availability'] = sample['availability'].value
            
            print("Currency after fix:", sample.get('currency'))
            
        return parsed_books
        
    except Exception as e:
        print(f"❌ Data preparation failed: {e}")
        return None

if __name__ == "__main__":
    # Test 1: Basic Spark
    test_spark_simple()
    
    # Test 2: Data preparation
    parsed_data = test_data_preparation()
    
    if parsed_data:
        # Test 3: Spark transformation with preprocessed data
        print("\n🧪 Testing Spark transformation with preprocessed data...")
        try:
            from processing.spark_transformer import SparkBookTransformer
            
            # Preprocess data to avoid enum issues
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
            
            # Test Spark transformation
            transformer = SparkBookTransformer()
            clean_df = transformer.transform_books_toscrape(processed_data)
            
            if clean_df:
                print("✅ Spark transformation successful!")
                clean_df.select("title", "price_current", "currency", "availability").show(3)
                print(f"Total records: {clean_df.count()}")
            
            transformer.stop_spark()
            
        except Exception as e:
            print(f"❌ Spark transformation failed: {e}")
            import traceback
            traceback.print_exc()