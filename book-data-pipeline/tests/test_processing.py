import pytest
from src.processing.matching import BookMatcher
from pyspark.sql import SparkSession

class TestMatching:
    @pytest.fixture
    def spark(self):
        return SparkSession.builder.appName("Test").getOrCreate()
    
    def test_matching_initialization(self, spark):
        matcher = BookMatcher(spark)
        assert matcher.matching_config['title_similarity_threshold'] == 0.8