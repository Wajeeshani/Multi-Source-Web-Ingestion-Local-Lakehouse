"""Common utilities for web ingestion"""
import hashlib
import json
from typing import Any, Dict

def generate_content_hash(content: str) -> str:
    """Generate MD5 hash for content deduplication"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def save_raw_data(raw_books: list, output_path: str):
    """Save raw book data to JSON file for testing"""
    serializable_data = []
    for book in raw_books:
        if hasattr(book, 'dict'):  # Pydantic model
            serializable_data.append(book.dict())
        else:
            serializable_data.append(book)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(serializable_data, f, indent=2, default=str)

def load_raw_data(input_path: str) -> list:
    """Load raw book data from JSON file"""
    with open(input_path, 'r', encoding='utf-8') as f:
        return json.load(f)