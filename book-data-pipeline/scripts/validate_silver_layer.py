#!/usr/bin/env python3
"""
Validate the data in the Silver layer
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def validate_silver_layer():
    """Validate the Silver layer data"""
    print("🔍 Validating Silver Layer Data")
    print("=" * 50)
    
    spark = SparkSession.builder \
        .appName("SilverLayerValidation") \
        .getOrCreate()
    
    try:
        # Read the saved data
        silver_path = "/opt/airflow/storage/silver/books_toscrape"
        df = spark.read.parquet(f"{silver_path}/*")
        
        print(f"📊 Total records: {df.count()}")
        print(f"🏗️  Schema: {len(df.columns)} columns")
        
        # Basic validation
        print("\n📈 Data Quality Summary:")
        print("-" * 30)
        
        # Check for nulls in critical fields
        critical_fields = ["title", "price_current", "source_id"]
        for field in critical_fields:
            null_count = df.filter(col(field).isNull()).count()
            print(f"   {field}: {null_count} nulls")
        
        # Data distribution
        print("\n💰 Price Statistics:")
        price_stats = df.select(
            mean("price_current").alias("avg_price"),
            min("price_current").alias("min_price"),
            max("price_current").alias("max_price")
        ).collect()[0]
        print(f"   Average: £{price_stats['avg_price']:.2f}")
        print(f"   Range: £{price_stats['min_price']:.2f} - £{price_stats['max_price']:.2f}")
        
        print("\n⭐ Rating Distribution:")
        rating_stats = df.select(
            mean("rating").alias("avg_rating"),
            count("rating").alias("rated_books")
        ).collect()[0]
        print(f"   Average Rating: {rating_stats['avg_rating']:.1f}/5.0")
        print(f"   Books with ratings: {rating_stats['rated_books']}/{df.count()}")
        
        print("\n📚 Category Breakdown:")
        category_counts = df.groupBy("category_path").count().orderBy(desc("count"))
        category_counts.show(truncate=False)
        
        print("✅ Silver layer validation completed!")
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    validate_silver_layer()