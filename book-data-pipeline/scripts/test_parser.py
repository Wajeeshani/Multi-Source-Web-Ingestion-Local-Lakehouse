#!/usr/bin/env python3
"""
Test the HTML parser functionality
"""

import sys
import json
from datetime import datetime
sys.path.insert(0, '/opt/airflow/src')

from ingestion.books_toscrape import scrape_and_parse_sample

def test_parser():
    """Test the HTML parser with live scraping"""
    print("🧪 Testing HTML parser with live data...")
    print("=" * 60)
    
    try:
        # Scrape and parse a small sample
        print("📚 Scraping and parsing sample books...")
        print("⏳ This may take a moment...")
        
        parsed_books = scrape_and_parse_sample(
            max_categories=1,  # Just one category
            max_pages=1        # Just first page
        )
        
        print(f"✅ Successfully parsed {len(parsed_books)} books")
        print("=" * 60)
        
        if parsed_books:
            # Display sample book data
            sample = parsed_books[0]
            print("📖 Sample parsed book data:")
            print(f"   Title: {sample.get('title', 'N/A')}")
            print(f"   Authors: {', '.join(sample.get('authors', []))}")
            print(f"   Price: {sample.get('price_current', 'N/A')} {sample.get('currency', 'N/A')}")
            print(f"   Availability: {sample.get('availability', 'N/A')}")
            print(f"   Rating: {sample.get('rating', 'N/A')}/5.0")
            print(f"   Category: {sample.get('category', 'N/A')}")
            print(f"   UPC: {sample.get('upc', 'N/A')}")
            
            # Show product info summary
            product_info = sample.get('product_info', {})
            print(f"   Product info fields: {len(product_info)}")
            for key in list(product_info.keys())[:3]:  # Show first 3
                print(f"     - {key}: {product_info[key][:50]}...")
            
            # Save sample to file for inspection
            import os
            os.makedirs('/opt/airflow/storage/bronze', exist_ok=True)
            output_file = '/opt/airflow/storage/bronze/parsed_books_sample.json'
            
            with open(output_file, 'w', encoding='utf-8') as f:
                # Convert datetime objects to strings
                serializable_books = []
                for book in parsed_books:
                    book_copy = book.copy()
                    if 'ingested_at' in book_copy and isinstance(book_copy['ingested_at'], datetime):
                        book_copy['ingested_at'] = book_copy['ingested_at'].isoformat()
                    serializable_books.append(book_copy)
                
                json.dump(serializable_books, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Sample data saved to: {output_file}")
            
        else:
            print("❌ No books were parsed. Possible issues:")
            print("   - Network connectivity")
            print("   - HTML structure changes")
            print("   - Parser logic issues")
            
        return parsed_books
        
    except Exception as e:
        print(f"❌ Error during parser test: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    test_parser()