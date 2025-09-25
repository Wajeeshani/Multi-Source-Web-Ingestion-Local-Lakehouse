from enum import Enum

# Data Quality Rules
class DQRuleType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    REGEX = "regex"
    CUSTOM = "custom"

# Field Mappings between sources
FIELD_MAPPINGS = {
    "books_toscrape": {
        "title": "title",
        "price": "price_current", 
        "availability": "availability",
        "rating": "rating",
        # ... more mappings
    },
    "it_bookstore": {
        "title": "title",
        "price": "price_current",
        "url": "product_url",
        # ... more mappings
    }
}

# Matching Configuration
MATCHING_CONFIG = {
    "exact_match_fields": ["isbn13"],
    "fuzzy_match_fields": ["title", "authors"],
    "title_similarity_threshold": 0.85,
    "author_similarity_threshold": 0.75,
    "min_confidence_score": 0.7
}

# Category Standardization
CATEGORY_MAPPINGS = {
    "books_toscrape": {
        "default_category": "Unknown",
        "mappings": {
            "travel": "Travel & Tourism",
            "music": "Arts & Music",
            # ... more category mappings
        }
    }
}