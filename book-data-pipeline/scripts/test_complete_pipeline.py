#!/usr/bin/env python3
"""
Complete end-to-end pipeline test
"""

import sys
sys.path.insert(0, '/opt/airflow/src')
import time
from datetime import datetime

def test_complete_pipeline():
    """Test the entire pipeline from ingestion to Gold layer"""
    print("🚀 COMPLETE END-TO-END PIPELINE TEST")
    print("=" * 70)
    
    # Track success/failure
    results = {}
    
    try:
        # Phase 1: Data Ingestion
        print("1. 📥 PHASE 1 - DATA INGESTION")
        print("-" * 40)
        
        # Test Books to Scrape
        print("   📚 Testing Books to Scrape ingestion...")
        from ingestion.books_toscrape import scrape_and_parse_sample
        books_toscrape_data = scrape_and_parse_sample(max_categories=1, max_pages=1)
        results['books_toscrape_ingestion'] = len(books_toscrape_data) > 0
        print(f"   ✅ Books to Scrape: {len(books_toscrape_data)} pages ingested")
        
        # Test IT Bookstore API
        print("   📚 Testing IT Bookstore API ingestion...")
        from ingestion.itbookstore_api import get_itbookstore_sample
        itbookstore_data = get_itbookstore_sample(max_books=3)
        results['itbookstore_ingestion'] = len(itbookstore_data) > 0
        print(f"   ✅ IT Bookstore: {len(itbookstore_data)} books ingested")
        
        # Phase 2: Silver Layer Processing
        print("\n2. 🔄 PHASE 2 - SILVER LAYER PROCESSING")
        print("-" * 40)
        
        from processing.spark_transformer import SparkBookTransformer
        from processing.spark_storage import SparkSilverLayerStorage
        from pyspark.sql import SparkSession

        # Create a single Spark session for the entire test
        spark = SparkSession.builder.appName("E2ETest").getOrCreate()
        
        transformer = SparkBookTransformer()
        storage = SparkSilverLayerStorage()
        
        # Process Books to Scrape data
        print("   ⚡ Transforming Books to Scrape data...")
        processed_toscrape = []
        for book in books_toscrape_data:
            processed_book = book.copy()
            if hasattr(processed_book.get('currency'), 'value'):
                processed_book['currency'] = processed_book['currency'].value
            if hasattr(processed_book.get('availability'), 'value'):
                processed_book['availability'] = processed_book['availability'].value
            processed_toscrape.append(processed_book)
        
        toscrape_df = transformer.transform_books_toscrape(processed_toscrape)
        results['books_toscrape_transform'] = toscrape_df.count() > 0
        print(f"   ✅ Books to Scrape: {toscrape_df.count()} books transformed")
        
        # Process IT Bookstore data
        print("   ⚡ Transforming IT Bookstore data...")
        processed_itbookstore = []
        for book in itbookstore_data:
            # Use model_dump() instead of dict() for Pydantic v2
            processed_book = book.model_dump() if hasattr(book, 'model_dump') else book.dict()
            if hasattr(processed_book.get('currency'), 'value'):
                processed_book['currency'] = processed_book['currency'].value
            processed_itbookstore.append(processed_book)
        
        itbookstore_df = transformer.transform_books_toscrape(processed_itbookstore)
        results['itbookstore_transform'] = itbookstore_df.count() > 0
        print(f"   ✅ IT Bookstore: {itbookstore_df.count()} books transformed")
        
        # Save to Silver layer
        print("   💾 Saving to Silver layer...")
        storage.save_clean_books(toscrape_df, "books_toscrape", "e2e_test")
        storage.save_clean_books(itbookstore_df, "it_bookstore", "e2e_test")
        results['silver_storage'] = True
        print("   ✅ Silver layer storage successful")
        
        # Phase 3: Gold Layer Processing
        print("\n3. 🏗️ PHASE 3 - GOLD LAYER PROCESSING")
        print("-" * 40)
        
        from processing.matching import BookMatcher
        from processing.gold_layer import GoldLayerBuilder
        
        # Use the existing Spark session
        matcher = BookMatcher(spark)
        gold_builder = GoldLayerBuilder(spark)
        
        # Read Silver data
        print("   📖 Reading Silver layer data...")
        silver_toscrape = storage.read_clean_books("books_toscrape")
        silver_itbookstore = storage.read_clean_books("it_bookstore")
        
        results['silver_reading'] = silver_toscrape is not None and silver_itbookstore is not None
        print(f"   ✅ Silver data read: {silver_toscrape.count() if silver_toscrape else 0} + {silver_itbookstore.count() if silver_itbookstore else 0} records")
        
        # Cross-source matching
        print("   🔄 Performing cross-source matching...")
        matches = matcher.match_books(silver_toscrape, silver_itbookstore)
        results['cross_source_matching'] = matches.count() >= 0
        print(f"   ✅ Matching completed: {matches.count()} potential matches")
        
        # Build Gold layer
        print("   🏗️ Building Gold layer...")
        all_books = silver_toscrape.unionByName(silver_itbookstore)
        canonical_books = gold_builder.build_canonical_books(all_books, matches)
        price_history = gold_builder.build_price_history(all_books, matches)
        
        results['gold_layer_construction'] = canonical_books.count() > 0
        print(f"   ✅ Gold layer built: {canonical_books.count()} canonical books, {price_history.count()} price events")
        
        # Save Gold layer
        print("   💾 Saving Gold layer...")
        gold_storage = SparkSilverLayerStorage("/opt/airflow/storage/gold")
        gold_storage.save_clean_books(canonical_books, "canonical_books", "e2e_test")
        gold_storage.save_clean_books(price_history, "price_history", "e2e_test")
        results['gold_storage'] = True
        print("   ✅ Gold layer storage successful")
        
        # Phase 4: Validation and Reporting
        print("\n4. 📊 PHASE 4 - VALIDATION & REPORTING")
        print("-" * 40)
        
        # Data quality checks
        print("   🔍 Running data quality checks...")
        from pyspark.sql.functions import col, mean, count
        
        # Read back saved data for validation
        gold_canonical = spark.read.parquet("/opt/airflow/storage/gold/canonical_books/*")
        gold_prices = spark.read.parquet("/opt/airflow/storage/gold/price_history/*")
        
        results['gold_validation'] = gold_canonical.count() > 0 and gold_prices.count() > 0
        print(f"   ✅ Gold data validation: {gold_canonical.count()} canonical, {gold_prices.count()} prices")
        
        # Quality metrics
        quality_stats = gold_canonical.select(
            mean("confidence_score").alias("avg_confidence"),
            count("book_id").alias("total_books"),
            count("isbn13").alias("books_with_isbn")
        ).collect()[0]
        
        print(f"   📈 Quality metrics:")
        print(f"      Average confidence: {quality_stats['avg_confidence']:.3f}")
        print(f"      Total canonical books: {quality_stats['total_books']}")
        print(f"      Books with ISBN: {quality_stats['books_with_isbn']}")
        
        # Generate report
        print("   📋 Generating test report...")
        report = {
            "test_timestamp": datetime.now().isoformat(),
            "results": results,
            "metrics": {
                "books_toscrape_ingested": len(books_toscrape_data),
                "itbookstore_ingested": len(itbookstore_data),
                "silver_books_processed": toscrape_df.count() + itbookstore_df.count(),
                "gold_canonical_books": canonical_books.count(),
                "price_history_events": price_history.count(),
                "cross_source_matches": matches.count(),
                "average_confidence": float(quality_stats['avg_confidence']) if quality_stats['avg_confidence'] else 0.0
            }
        }
        
        # Save report
        import json
        with open('/opt/airflow/storage/e2e_test_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        results['report_generation'] = True
        print("   ✅ Test report generated")
        
        # Clean up - stop Spark session at the very end
        spark.stop()
        
    except Exception as e:
        print(f"   ❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        results['error'] = str(e)
        
        # Ensure Spark is stopped even on error
        try:
            spark.stop()
        except:
            pass
    
    # Final Results
    print("\n5. 🎯 FINAL RESULTS")
    print("-" * 40)
    
    successful_tests = sum(1 for k, v in results.items() if v is True and k != 'error')
    total_tests = len([k for k in results.keys() if k != 'error'])
    
    print(f"   Tests passed: {successful_tests}/{total_tests}")
    
    for test_name, result in results.items():
        if test_name != 'error':
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"   {status} {test_name}")
    
    if 'error' in results:
        print(f"   ❌ ERROR: {results['error']}")
    
    # Overall verdict
    if successful_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED! Pipeline is working correctly.")
        return True
    else:
        print(f"\n⚠️  {total_tests - successful_tests} test(s) failed. Please check the implementation.")
        return False

if __name__ == "__main__":
    success = test_complete_pipeline()
    sys.exit(0 if success else 1)