#!/usr/bin/env python3
"""
Display cross-source matching results for demonstration
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

from pyspark.sql import SparkSession

def show_matches():
    """Display matched books from Gold layer"""
    spark = SparkSession.builder.appName("ShowMatches").getOrCreate()
    
    try:
        # Read canonical books
        canonical_path = "/opt/airflow/storage/gold/canonical_books/*"
        canonical_df = spark.read.parquet(canonical_path)
        
        print("📚 Canonical Books (Cross-Source Matches)")
        print("=" * 60)
        
        # Show books with multiple sources first
        multi_source = canonical_df.filter(col("source_count") > 1)
        single_source = canonical_df.filter(col("source_count") == 1)
        
        print(f"🔗 Multi-source matches: {multi_source.count()} books")
        if multi_source.count() > 0:
            multi_source.select(
                "book_id", "title", "source_count", "confidence_score"
            ).show(10, truncate=30)
        
        print(f"📖 Single-source books: {single_source.count()} books")
        if single_source.count() > 0:
            single_source.select(
                "book_id", "title", "source_count"
            ).show(5, truncate=30)
        
        # Show price history sample
        price_path = "/opt/airflow/storage/gold/price_history/*"
        price_df = spark.read.parquet(price_path)
        
        print(f"💰 Price history events: {price_df.count()}")
        price_df.groupBy("source").count().show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    show_matches()