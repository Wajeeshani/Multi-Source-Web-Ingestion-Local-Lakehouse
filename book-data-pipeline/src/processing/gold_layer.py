from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from typing import List, Dict

class GoldLayerBuilder:
    """Build the curated Gold layer with SCD Type 2 dimensions"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def build_canonical_books(self, silver_books: DataFrame, matches: DataFrame) -> DataFrame:
        """Build canonical book dimension with SCD Type 2"""
        
        # Create unified book records from matches
        canonical_books = self._create_unified_records(silver_books, matches)
        
        # Add SCD Type 2 metadata
        scd_books = canonical_books.transform(self._add_scd_metadata)
        
        return scd_books
    
    def _create_unified_records(self, silver_books: DataFrame, matches: DataFrame) -> DataFrame:
        """Create unified book records from matched pairs using PySpark create_map()"""
        
        # Split silver books by source
        books_toscrape = silver_books.filter(col("source") == "books_toscrape")
        it_bookstore = silver_books.filter(col("source") == "it_bookstore")
        
        # --- Matched records ---
        matched_records = matches.select(
            concat_ws("_", col("source1_id"), col("source2_id")).alias("book_id"),
            coalesce(col("source1_isbn13"), col("source2_isbn13")).alias("isbn13"),
            coalesce(col("source1_title"), col("source2_title")).alias("title"),
            coalesce(col("source1_authors"), col("source2_authors")).alias("authors"),
            coalesce(col("source1_category"), col("source2_category")).alias("primary_category"),
            col("confidence_score"),
            lit(2).alias("source_count"),
            create_map(
                lit("books_toscrape"), col("source1_id"),
                lit("it_bookstore"), col("source2_id")
            ).alias("source_ids")
        )
        
        # --- Unmatched books from books_toscrape ---
        matched_toscrape_ids = [row['source1_id'] for row in matches.select("source1_id").distinct().collect()]
        unmatched_toscrape = books_toscrape.filter(~col("source_id").isin(matched_toscrape_ids)).select(
            concat_ws("_", col("source_id"), lit("single")).alias("book_id"),
            col("isbn13"),
            col("title"),
            col("authors"),
            split(col("category_path"), " > ").getItem(1).alias("primary_category"),
            lit(1.0).alias("confidence_score"),
            lit(1).alias("source_count"),
            create_map(lit("books_toscrape"), col("source_id")).alias("source_ids")
        )
        
        # --- Unmatched books from it_bookstore ---
        matched_it_ids = [row['source2_id'] for row in matches.select("source2_id").distinct().collect()]
        unmatched_itbookstore = it_bookstore.filter(~col("source_id").isin(matched_it_ids)).select(
            concat_ws("_", lit("single"), col("source_id")).alias("book_id"),
            col("isbn13"),
            col("title"),
            col("authors"),
            split(col("category_path"), " > ").getItem(1).alias("primary_category"),
            lit(1.0).alias("confidence_score"),
            lit(1).alias("source_count"),
            create_map(lit("it_bookstore"), col("source_id")).alias("source_ids")
        )
        
        # --- Combine all records ---
        return matched_records.unionByName(unmatched_toscrape).unionByName(unmatched_itbookstore)
    
    def _add_scd_metadata(self, df: DataFrame) -> DataFrame:
        """Add SCD Type 2 metadata"""
        current_timestamp = datetime.now()
        
        return df.withColumn("valid_from", lit(current_timestamp).cast(TimestampType())) \
                .withColumn("valid_to", lit(None).cast(TimestampType())) \
                .withColumn("is_current", lit(True)) \
                .withColumn("version", lit(1))
    
    def build_price_history(self, silver_books: DataFrame, matches: DataFrame) -> DataFrame:
        """Build price history fact table"""
        
        # Extract price events from both sources
        price_events_toscrape = silver_books.filter(col("source") == "books_toscrape") \
            .select(
                col("source_id").alias("book_source_id"),
                col("source").alias("book_source"),
                col("price_current"),
                col("currency"),
                col("ingested_at").alias("effective_date"),
                lit("books_toscrape").alias("price_source")
            )
        
        price_events_itbookstore = silver_books.filter(col("source") == "it_bookstore") \
            .select(
                col("source_id").alias("book_source_id"),
                col("source").alias("book_source"),
                col("price_current"),
                col("currency"),
                col("ingested_at").alias("effective_date"),
                lit("it_bookstore").alias("price_source")
            )
        
        # Combine price events
        all_price_events = price_events_toscrape.unionByName(price_events_itbookstore)
        
        # Map to canonical book IDs using matches
        if matches.count() > 0:
            # Create mapping from source IDs to canonical book IDs
            source1_mapping = matches.select(
                col("source1_id").alias("book_source_id"),
                concat_ws("_", col("source1_id"), col("source2_id")).alias("canonical_book_id")
            )
            
            source2_mapping = matches.select(
                col("source2_id").alias("book_source_id"),
                concat_ws("_", col("source1_id"), col("source2_id")).alias("canonical_book_id")
            )
            
            mapping = source1_mapping.unionByName(source2_mapping)
            
            # Join with price events
            price_history = all_price_events.join(mapping, "book_source_id", "left") \
                .withColumn("canonical_book_id", 
                           when(col("canonical_book_id").isNull(), 
                                concat_ws("_", col("book_source_id"), lit("single")))
                           .otherwise(col("canonical_book_id")))
        else:
            price_history = all_price_events.withColumn(
                "canonical_book_id", concat_ws("_", col("book_source_id"), lit("single"))
            )
        
        return price_history.select(
            "canonical_book_id",
            "price_source",
            "price_current",
            "currency",
            "effective_date"
        )