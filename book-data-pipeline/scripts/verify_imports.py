#!/usr/bin/env python3
"""
Verify all imports are working
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

print("🔍 Verifying imports...")

try:
    # Test utility imports
    from utils.config import settings
    print("✅ utils.config import successful")
    
    from utils.schemas import SourceType, CleanBookData
    print("✅ utils.schemas import successful")
    
    # Test ingestion imports
    from ingestion.books_toscrape import BooksToScrapeScraper
    print("✅ ingestion.books_toscrape import successful")
    
    from ingestion.itbookstore_api import ITBookstoreAPI
    print("✅ ingestion.itbookstore_api import successful")
    
    # Test processing imports
    from processing.spark_transformer import SparkBookTransformer
    print("✅ processing.spark_transformer import successful")
    
    from processing.spark_storage import SparkSilverLayerStorage
    print("✅ processing.spark_storage import successful")
    
    from processing.matching import BookMatcher
    print("✅ processing.matching import successful")
    
    from processing.gold_layer import GoldLayerBuilder
    print("✅ processing.gold_layer import successful")
    
    # Test PySpark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print("✅ PySpark import and session creation successful")
    spark.stop()
    
    print("\n🎉 All imports successful!")
    
except Exception as e:
    print(f"❌ Import failed: {e}")
    import traceback
    traceback.print_exc()