from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def debug_storage():
    """Debug storage paths and permissions"""
    print("=== STORAGE DEBUG INFORMATION ===")
    
    # Check storage directory
    storage_paths = [
        '/opt/airflow/storage',
        '/opt/airflow/storage/silver', 
        '/opt/airflow/storage/gold',
        '/opt/airflow/storage/silver/books_toscrape',
        '/opt/airflow/storage/silver/it_bookstore',
        '/opt/airflow/storage/gold/canonical_books',
        '/opt/airflow/storage/gold/price_history'
    ]
    
    for path in storage_paths:
        exists = os.path.exists(path)
        is_dir = os.path.isdir(path) if exists else False
        if exists:
            permissions = oct(os.stat(path).st_mode)[-3:]
            files = os.listdir(path) if is_dir else []
            print(f"✅ {path}: exists, permissions {permissions}, files: {len(files)}")
            if files:
                for f in files[:3]:  # Show first 3 files
                    print(f"   - {f}")
        else:
            print(f"❌ {path}: does not exist")
    
    # Check if we can write
    test_file = '/opt/airflow/storage/test_write.txt'
    try:
        with open(test_file, 'w') as f:
            f.write(f"Test write at {datetime.now()}")
        os.remove(test_file)
        print("✅ Write permissions: OK")
    except Exception as e:
        print(f"❌ Write permissions failed: {e}")
    
    return "Storage debug completed"

def test_spark_write():
    """Test Spark write functionality"""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    print("=== SPARK WRITE TEST ===")
    
    spark = None
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("StorageTest") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Create test data
        data = [("Book1", "Author1", 100), ("Book2", "Author2", 200)]
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("price", IntegerType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        print(f"Created test DataFrame with {df.count()} rows")
        
        # Test write to storage
        test_path = "/opt/airflow/storage/test_spark_output"
        df.write.mode("overwrite").parquet(test_path)
        
        # Test read back
        df_read = spark.read.parquet(test_path)
        print(f"Read back {df_read.count()} rows from {test_path}")
        
        # Cleanup
        import shutil
        if os.path.exists(test_path):
            shutil.rmtree(test_path)
            
        return "Spark write test: SUCCESS"
        
    except Exception as e:
        print(f"Spark write test failed: {e}")
        import traceback
        traceback.print_exc()
        return f"Spark write test: FAILED - {e}"
    finally:
        if spark:
            spark.stop()

with DAG(
    dag_id='debug_storage',
    default_args=default_args,
    description='Debug storage and Spark issues',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    debug_task = PythonOperator(
        task_id='debug_storage',
        python_callable=debug_storage,
    )
    
    spark_test_task = PythonOperator(
        task_id='test_spark_write',
        python_callable=test_spark_write,
    )
    
    debug_task >> spark_test_task