import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("=== Basic Spark Storage Test ===")

# Check storage directory
storage_path = "/opt/airflow/storage"
print(f"Storage path: {storage_path}")
print(f"Exists: {os.path.exists(storage_path)}")
print(f"Is directory: {os.path.isdir(storage_path) if os.path.exists(storage_path) else 'N/A'}")

if os.path.exists(storage_path):
    print("Contents:")
    for item in os.listdir(storage_path):
        print(f"  - {item}")

# Test Spark
try:
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    print("Spark session created successfully")
    
    # Create simple DataFrame
    data = [("Book1", "Author1", 100), ("Book2", "Author2", 200)]
    df = spark.createDataFrame(data, ["title", "author", "price"])
    print(f"DataFrame created with {df.count()} rows")
    
    # Test write to different locations
    test_paths = [
        "/opt/airflow/storage/test_output",
        "/tmp/test_output"
    ]
    
    for path in test_paths:
        print(f"\nTesting write to: {path}")
        try:
            df.write.mode("overwrite").parquet(path)
            print("✅ Write successful")
            
            if os.path.exists(path):
                files = os.listdir(path)
                print(f"✅ Files created: {len(files)}")
            else:
                print("❌ Path not created")
                
        except Exception as e:
            print(f"❌ Write failed: {e}")
    
    spark.stop()
    print("Spark session stopped")
    
except Exception as e:
    print(f"❌ Spark test failed: {e}")
    import traceback
    traceback.print_exc()