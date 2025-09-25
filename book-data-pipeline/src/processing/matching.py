from typing import List, Dict, Tuple, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import jellyfish  # For fuzzy string matching
from datetime import datetime

class BookMatcher:
    """Match books from different sources using various strategies"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.matching_config = {
            'title_similarity_threshold': 0.85,
            'author_similarity_threshold': 0.75,
            'isbn_exact_match_weight': 1.0,
            'title_fuzzy_match_weight': 0.7,
            'author_fuzzy_match_weight': 0.3,
            'min_confidence_score': 0.6
        }
    
    def match_books(self, books_df1: DataFrame, books_df2: DataFrame) -> DataFrame:
        """Match books between two DataFrames"""
        
        # Prepare data for matching
        df1_clean = self._prepare_for_matching(books_df1, 'source1')
        df2_clean = self._prepare_for_matching(books_df2, 'source2')
        
        # Create cross join for comparison
        cross_joined = df1_clean.crossJoin(df2_clean)
        
        # Calculate matching scores
        scored_matches = cross_joined.transform(self._calculate_matching_scores)
        
        # Filter confident matches
        confident_matches = scored_matches.filter(
            col("confidence_score") >= self.matching_config['min_confidence_score']
        )
        
        # Select best matches (deduplicate)
        best_matches = self._select_best_matches(confident_matches)
        
        return best_matches
    
    def _prepare_for_matching(self, df: DataFrame, source_alias: str) -> DataFrame:
        """Prepare DataFrame for matching by cleaning and standardizing"""
        return df.select(
            col("source").alias(f"{source_alias}_source"),
            col("source_id").alias(f"{source_alias}_id"),
            col("isbn13").alias(f"{source_alias}_isbn13"),
            lower(col("title")).alias(f"{source_alias}_title"),
            col("authors").alias(f"{source_alias}_authors"),
            col("price_current").alias(f"{source_alias}_price"),
            col("currency").alias(f"{source_alias}_currency"),
            col("category_path").alias(f"{source_alias}_category"),
            col("product_url").alias(f"{source_alias}_url")
        ).filter(col(f"{source_alias}_title").isNotNull())
    
    def _calculate_matching_scores(self, df: DataFrame) -> DataFrame:
        """Calculate matching scores between book pairs"""
        
        # ISBN exact match
        isbn_match = when(
            (col("source1_isbn13").isNotNull() & 
             col("source2_isbn13").isNotNull() & 
             (col("source1_isbn13") == col("source2_isbn13"))),
            self.matching_config['isbn_exact_match_weight']
        ).otherwise(0.0)
        
        # Title similarity
        title_similarity = self._calculate_string_similarity(
            col("source1_title"), col("source2_title")
        )
        title_match = when(
            title_similarity >= self.matching_config['title_similarity_threshold'],
            title_similarity * self.matching_config['title_fuzzy_match_weight']
        ).otherwise(0.0)
        
        # Author similarity (if authors available)
        author_match = when(
            (col("source1_authors").isNotNull() & col("source2_authors").isNotNull()),
            self._calculate_author_similarity(col("source1_authors"), col("source2_authors"))
        ).otherwise(0.0)
        
        # Calculate total confidence score
        confidence_score = isbn_match + title_match + author_match
        
        return df.withColumn("isbn_match_score", isbn_match) \
                .withColumn("title_similarity", title_similarity) \
                .withColumn("title_match_score", title_match) \
                .withColumn("author_match_score", author_match) \
                .withColumn("confidence_score", confidence_score) \
                .withColumn("match_type", 
                           when(col("isbn_match_score") > 0, "exact_isbn")
                           .when(col("title_match_score") > 0, "fuzzy_title")
                           .otherwise("low_confidence"))
    
    def _calculate_string_similarity(self, str1, str2):
        """Calculate similarity between two strings using Jaro-Winkler"""
        @udf(returnType=DoubleType())
        def similarity_udf(s1, s2):
            if s1 is None or s2 is None:
                return 0.0
            return jellyfish.jaro_winkler_similarity(str(s1), str(s2))
        
        return similarity_udf(str1, str2)
    
    def _calculate_author_similarity(self, authors1, authors2):
        """Calculate similarity between author lists"""
        @udf(returnType=DoubleType())
        def author_similarity_udf(a1, a2):
            if not a1 or not a2:
                return 0.0
            
            # Convert to sets for comparison
            set1 = set(a1)
            set2 = set(a2)
            
            if not set1 or not set2:
                return 0.0
            
            # Jaccard similarity
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            
            if union == 0:
                return 0.0
            
            return intersection / union
        
        return author_similarity_udf(authors1, authors2) * self.matching_config['author_fuzzy_match_weight']
    
    def _select_best_matches(self, matches_df: DataFrame) -> DataFrame:
        """Select the best match for each book (deduplicate)"""
        from pyspark.sql.window import Window
        
        # For each source1 book, select the best match from source2
        window_spec = Window.partitionBy("source1_id").orderBy(desc("confidence_score"))
        
        best_matches = matches_df.withColumn("rank", row_number().over(window_spec)) \
                                .filter(col("rank") == 1) \
                                .drop("rank")
        
        return best_matches