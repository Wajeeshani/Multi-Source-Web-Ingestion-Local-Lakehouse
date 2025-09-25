from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from pathlib import Path
from typing import List

class SparkSilverLayerStorage:
    """Save cleaned data to Silver layer using PySpark"""
    
    def __init__(self, base_path: str = "/opt/airflow/storage/silver"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        self.spark = SparkSession.builder \
            .appName("SilverLayerStorage") \
            .getOrCreate()
    
    def save_clean_books(self, clean_df: DataFrame, source: str, batch_id: str = None):
        """Save clean books to Parquet using Spark"""
        if clean_df.rdd.isEmpty():
            print("No data to save")
            return None
        
        if batch_id is None:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output path
        output_path = self.base_path / source / f"batch_{batch_id}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Write with partitioning by source
        clean_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(str(output_path))
        
        print(f"✅ Saved {clean_df.count()} records to: {output_path}")
        
        # Save sample as JSON for inspection
        sample_df = clean_df.limit(3)
        sample_path = self.base_path / f"sample_{source}_{batch_id}.json"
        sample_df.write.mode("overwrite").json(str(sample_path))
        
        print(f"✅ Sample saved to: {sample_path}")
        
        return output_path
    
    def read_clean_books(self, source: str = None):
        """Read clean books from Silver layer"""
        if source:
            path = self.base_path / source
        else:
            path = self.base_path
        
        if not path.exists():
            print(f"No data found at: {path}")
            return None
        
        # Read all Parquet files
        return self.spark.read.parquet(str(path / "*"))
    
    def stop_spark(self):
        """Stop Spark session"""
        self.spark.stop()