from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List, Dict
import json
from datetime import datetime

class SparkBookTransformer:
    """Transform raw parsed data into CleanBookData schema using PySpark"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BookDataTransformer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Define the CleanBookData schema explicitly
        self.clean_book_schema = StructType([
            StructField("source", StringType(), False),
            StructField("source_id", StringType(), False),
            StructField("isbn13", StringType(), True),
            StructField("title", StringType(), False),
            StructField("authors", ArrayType(StringType()), True),
            StructField("category_path", StringType(), False),
            StructField("price_current", DoubleType(), False),
            StructField("price_original", DoubleType(), True),
            StructField("currency", StringType(), False),
            StructField("availability", StringType(), False),
            StructField("stock_quantity", IntegerType(), True),
            StructField("rating", DoubleType(), True),
            StructField("num_reviews", IntegerType(), True),
            StructField("specs", MapType(StringType(), StringType()), True),
            StructField("image_url", StringType(), True),
            StructField("product_url", StringType(), False),
            StructField("description", StringType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("content_hash", StringType(), False),
            StructField("data_quality_score", DoubleType(), False)
        ])
    
    def transform_books_toscrape(self, parsed_data: List[Dict]) -> DataFrame:
        """Transform Books to Scrape parsed data to CleanBookData using Spark"""
        
        # Pre-process data to convert enums to strings
        processed_data = self._preprocess_data(parsed_data)
        
        # Create DataFrame with explicit schema to avoid inference issues
        parsed_df = self.spark.createDataFrame(processed_data, self._get_input_schema())
        
        # Apply transformations
        clean_df = parsed_df.transform(self._add_source_info) \
                           .transform(self._extract_authors_array) \
                           .transform(self._build_category_path) \
                           .transform(self._handle_prices) \
                           .transform(self._extract_availability) \
                           .transform(self._extract_stock_quantity) \
                           .transform(self._handle_specs_map) \
                           .transform(self._calculate_quality_score) \
                           .transform(self._generate_content_hash) \
                           .transform(self._select_final_columns)
        
        return clean_df
    
    def _preprocess_data(self, parsed_data: List[Dict]) -> List[Dict]:
        """Pre-process data to convert enums and complex objects to strings"""
        processed = []
        for item in parsed_data:
            # Create a copy and convert enums to strings
            processed_item = item.copy()
            
            # Convert enum values to strings
            if 'currency' in processed_item and processed_item['currency']:
                processed_item['currency'] = str(processed_item['currency'])
            
            if 'availability' in processed_item and processed_item['availability']:
                processed_item['availability'] = str(processed_item['availability'])
            
            # Ensure authors is a list of strings
            if 'authors' in processed_item:
                if isinstance(processed_item['authors'], str):
                    processed_item['authors'] = [processed_item['authors']]
                elif not isinstance(processed_item['authors'], list):
                    processed_item['authors'] = []
            
            # Convert ingested_at to string for Spark to parse
            if 'ingested_at' in processed_item and isinstance(processed_item['ingested_at'], datetime):
                processed_item['ingested_at'] = processed_item['ingested_at'].isoformat()
            
            processed.append(processed_item)
        
        return processed
    
    def _get_input_schema(self) -> StructType:
        """Define schema for the input parsed data"""
        return StructType([
            StructField("title", StringType(), True),
            StructField("authors", ArrayType(StringType()), True),
            StructField("price_current", DoubleType(), True),
            StructField("price_original", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("availability", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("num_reviews", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("product_info", MapType(StringType(), StringType()), True),
            StructField("upc", StringType(), True),
            StructField("image_url", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("ingested_at", StringType(), True),  # String for timestamp parsing
            StructField("http_status", IntegerType(), True)
        ])
    
    def _add_source_info(self, df: DataFrame) -> DataFrame:
        """Add source information"""
        return df.withColumn("source", lit("books_toscrape")) \
                .withColumn("source_id", coalesce(col("upc"), col("title"))) \
                .withColumn("isbn13", col("upc"))
    
    def _extract_authors_array(self, df: DataFrame) -> DataFrame:
        """Convert authors to array type"""
        return df.withColumn(
            "authors", 
            when(col("authors").isNull(), array())
            .when(size(col("authors")) == 0, array())
            .otherwise(col("authors"))
        )
    
    def _build_category_path(self, df: DataFrame) -> DataFrame:
        """Build category path hierarchy"""
        return df.withColumn(
            "category_path", 
            concat(lit("Books > "), 
                   when(col("category").isNull(), lit("Unknown"))
                   .otherwise(col("category")))
        )
    
    def _handle_prices(self, df: DataFrame) -> DataFrame:
        """Handle price and currency transformations"""
        return df.withColumn("price_current", col("price_current").cast("double")) \
                .withColumn("price_original", col("price_original").cast("double")) \
                .withColumn("currency", 
                           when(col("currency").isNull(), lit("GBP"))
                           .otherwise(col("currency")))
    
    def _extract_availability(self, df: DataFrame) -> DataFrame:
        """Extract and standardize availability status"""
        return df.withColumn(
            "availability",
            when(lower(col("availability")).contains("in stock"), lit("in_stock"))
            .when(lower(col("availability")).contains("out of stock"), lit("out_of_stock"))
            .otherwise(lit("unknown"))
        )
    
    def _extract_stock_quantity(self, df: DataFrame) -> DataFrame:
        """Extract stock quantity from availability text"""
        return df.withColumn(
            "stock_quantity",
            when(col("product_info").isNotNull(), 
                 regexp_extract(col("product_info").getItem("availability"), r"\((\d+) available\)", 1))
            .otherwise(lit(None))
        ).withColumn("stock_quantity", col("stock_quantity").cast("int"))
    
    def _handle_specs_map(self, df: DataFrame) -> DataFrame:
        """Convert product_info to map type for specs"""
        return df.withColumn(
            "specs", 
            when(col("product_info").isNull(), create_map())
            .otherwise(col("product_info"))
        )
    
    def _calculate_quality_score(self, df: DataFrame) -> DataFrame:
        """Calculate data quality score (0.0 to 1.0)"""
        base_score = 1.0
        
        return df.withColumn("quality_score", lit(base_score)) \
                .withColumn("quality_score", 
                           when(col("title").isNull(), col("quality_score") - 0.3)
                           .otherwise(col("quality_score"))) \
                .withColumn("quality_score", 
                           when(col("price_current").isNull(), col("quality_score") - 0.3)
                           .otherwise(col("quality_score"))) \
                .withColumn("quality_score", 
                           when(col("upc").isNull(), col("quality_score") - 0.2)
                           .otherwise(col("quality_score"))) \
                .withColumn("quality_score", 
                           when(col("category").isNull(), col("quality_score") - 0.1)
                           .otherwise(col("quality_score"))) \
                .withColumn("quality_score", 
                           when(size(col("authors")) == 0, col("quality_score") - 0.1)
                           .otherwise(col("quality_score"))) \
                .withColumn("data_quality_score", 
                           greatest(lit(0.0), least(lit(1.0), col("quality_score")))) \
                .drop("quality_score")
    
    def _generate_content_hash(self, df: DataFrame) -> DataFrame:
        """Generate content hash for deduplication"""
        return df.withColumn(
            "content_hash", 
            md5(concat_ws("_", col("title"), col("upc"), col("source_url")))
        )
    
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select and rename columns to match CleanBookData schema"""
        return df.select(
            col("source"),
            col("source_id"),
            col("isbn13"),
            col("title"),
            col("authors"),
            col("category_path"),
            col("price_current"),
            col("price_original"),
            col("currency"),
            col("availability"),
            col("stock_quantity"),
            col("rating"),
            col("num_reviews"),
            col("specs"),
            col("image_url"),
            col("source_url").alias("product_url"),
            col("description"),
            to_timestamp(col("ingested_at")).alias("ingested_at"),
            col("content_hash"),
            col("data_quality_score")
        )
    
    def validate_schema(self, df: DataFrame) -> bool:
        """Validate that DataFrame matches CleanBookData schema"""
        try:
            # Check if all expected columns are present
            expected_columns = set([field.name for field in self.clean_book_schema])
            actual_columns = set(df.columns)
            
            if expected_columns != actual_columns:
                missing = expected_columns - actual_columns
                extra = actual_columns - expected_columns
                print(f"Schema validation failed. Missing: {missing}, Extra: {extra}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Schema validation error: {e}")
            return False

    def stop_spark(self):
        """Stop Spark session"""
        self.spark.stop()