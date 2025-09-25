#!/usr/bin/env python3
"""
Final test for Gold layer pipeline
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

def test_gold_final():
    """Test the Gold layer pipeline"""
    print("🚀 Testing Gold Layer Pipeline")
    print("=" * 50)
    
    from pyspark.sql import SparkSession
    from processing.matching import BookMatcher
    from processing.gold_layer import GoldLayerBuilder
    from processing.spark_storage import SparkSilverLayerStorage
    from pyspark.sql.functions import *
    
    spark = SparkSession.builder \
        .appName("GoldLayerTest") \
        .getOrCreate()
    
    storage = SparkSilverLayerStorage()
    matcher = BookMatcher(spark)
    gold_builder = GoldLayerBuilder(spark)
    
    try:
        # Step 1: Read Silver layer data
        print("1. 📖 Reading Silver layer data...")
        books_toscrape = storage.read_clean_books("books_toscrape")
        
        if books_toscrape is None:
            print("   ❌ No Silver layer data found. Creating sample data...")
            from pyspark.sql.types import *
            sample_data = [
                ("book1", "Travel Guide", ["Author A"], 29.99, "GBP", "Books > Travel"),
                ("book2", "Python Programming", ["Author B"], 39.99, "USD", "Books > Technology"),
            ]
            schema = StructType([
                StructField("source_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("authors", ArrayType(StringType()), True),
                StructField("price_current", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("category_path", StringType(), True),
            ])
            books_toscrape = spark.createDataFrame(sample_data, schema) \
                .withColumn("source", lit("books_toscrape")) \
                .withColumn("isbn13", lit(None)) \
                .withColumn("product_url", lit("http://example.com")) \
                .withColumn("ingested_at", current_timestamp())
        
        print(f"   ✅ Source 1: {books_toscrape.count()} records")
        
        # Step 2: Create sample IT Bookstore data
        print("2. 📚 Creating sample IT Bookstore data...")
        from pyspark.sql.types import *
        
        sample_data = [
            ("9781912047451", "Python Programming Guide", ["John Smith"], 39.99, "USD", "Books > Technology"),
            ("9781234567890", "Data Engineering Fundamentals", ["Jane Doe"], 49.99, "USD", "Books > Technology"),
            ("9781111111111", "Travel Guide 2024", ["Author A"], 29.99, "USD", "Books > Travel"),
        ]
        
        sample_schema = StructType([
            StructField("isbn13", StringType(), True),
            StructField("title", StringType(), True),
            StructField("authors", ArrayType(StringType()), True),
            StructField("price_current", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("category_path", StringType(), True),
        ])
        
        it_bookstore = spark.createDataFrame(sample_data, sample_schema) \
            .withColumn("source", lit("it_bookstore")) \
            .withColumn("source_id", col("isbn13")) \
            .withColumn("product_url", lit("http://example.com")) \
            .withColumn("ingested_at", current_timestamp())
        
        print(f"   ✅ Source 2: {it_bookstore.count()} records")
        
        # Show sample data
        print("   Sample from Source 1:")
        books_toscrape.select("title", "authors", "price_current", "currency").show(3, truncate=30)
        
        print("   Sample from Source 2:")
        it_bookstore.select("title", "authors", "price_current", "currency").show(3, truncate=30)
        
        # Step 3: Cross-source matching
        print("3. 🔄 Performing cross-source matching...")
        matches = matcher.match_books(books_toscrape, it_bookstore)
        print(f"   ✅ Found {matches.count()} potential matches")
        
        if matches.count() > 0:
            print("   Top matches:")
            matches.select("source1_title", "source2_title", "confidence_score", "match_type").show(5, truncate=25)
        
        # Step 4: Build Gold layer
        print("4. 🏗️ Building Gold layer...")
        
        # Combine all books for Gold layer processing
        all_books = books_toscrape.select(
            "source", "source_id", "isbn13", "title", "authors", 
            "price_current", "currency", "category_path", "product_url", "ingested_at"
        ).unionByName(it_bookstore.select(
            "source", "source_id", "isbn13", "title", "authors",
            "price_current", "currency", "category_path", "product_url", "ingested_at"
        ))
        
        # Build canonical books dimension
        canonical_books = gold_builder.build_canonical_books(all_books, matches)
        print(f"   ✅ Canonical books: {canonical_books.count()} records")
        print("   Sample canonical books:")
        canonical_books.select("book_id", "title", "source_count", "confidence_score").show(5, truncate=25)
        
        # Build price history fact
        price_history = gold_builder.build_price_history(all_books, matches)
        print(f"   ✅ Price history: {price_history.count()} records")
        print("   Sample price history:")
        price_history.show(5)
        
        # Step 5: Save Gold layer
        print("5. 💾 Saving Gold layer...")
        gold_storage = SparkSilverLayerStorage("/opt/airflow/storage/gold")
        
        # Save canonical books
        canonical_path = gold_storage.save_clean_books(canonical_books, "canonical_books", "test")
        
        # Save price history
        price_path = gold_storage.save_clean_books(price_history, "price_history", "test")
        
        print(f"   ✅ Canonical books saved to: {canonical_path}")
        print(f"   ✅ Price history saved to: {price_path}")
        
        # Step 6: Summary
        print("6. 📊 Gold Layer Summary:")
        print(f"   📚 Total canonical books: {canonical_books.count()}")
        print(f"   💰 Price history events: {price_history.count()}")
        print(f"   🔗 Successful matches: {matches.count()}")
        
        if matches.count() > 0:
            avg_confidence = matches.select(mean("confidence_score")).collect()[0][0]
            print(f"   🎯 Average match confidence: {avg_confidence:.3f}")
        
        print("\n🎉 Gold Layer Pipeline Test COMPLETED!")
        
    except Exception as e:
        print(f"❌ Error in Gold layer pipeline: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    test_gold_final()