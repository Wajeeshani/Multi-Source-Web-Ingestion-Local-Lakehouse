#!/usr/bin/env python3
"""
Test the IT Bookstore API integration
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

from ingestion.itbookstore_api import ITBookstoreAPI, ITBookstoreParser

def test_itbookstore_api():
    """Test IT Bookstore API functionality"""
    print("🚀 Testing IT Bookstore API")
    print("=" * 50)
    
    api = ITBookstoreAPI(delay=0.5)
    
    try:
        # Test 1: Get new books
        print("1. 📚 Getting new books...")
        new_books = api.get_new_books(page=1)
        print(f"   ✅ Retrieved {len(new_books)} new books")
        
        if new_books:
            sample_book = new_books[0]
            print(f"   📖 Sample book: {sample_book.get('title', 'Unknown')}")
            print(f"   💰 Price: {sample_book.get('price', 'Unknown')}")
            print(f"   🔢 ISBN13: {sample_book.get('isbn13', 'Unknown')}")
        
        # Test 2: Get book details
        print("\n2. 🔍 Getting book details...")
        if new_books:
            isbn13 = new_books[0].get('isbn13')
            if isbn13:
                details = api.get_book_details(isbn13)
                if details:
                    print(f"   ✅ Retrieved details for: {details.get('title')}")
                    print(f"   📝 Description length: {len(details.get('desc', ''))} chars")
                    print(f"   🏢 Publisher: {details.get('publisher', 'Unknown')}")
        
        # Test 3: Parse book data
        print("\n3. 🔄 Parsing book data...")
        if new_books and isbn13:
            details = api.get_book_details(isbn13)
            if details:
                parsed_data = ITBookstoreParser.parse_book_data(details)
                if parsed_data:
                    print("   ✅ Successfully parsed book data:")
                    for key, value in list(parsed_data.items())[:8]:  # Show first 8 fields
                        print(f"      {key}: {value}")
        
        # Test 4: Get sample for pipeline
        print("\n4. 🎯 Testing sample collection...")
        raw_books = api.get_sample_books(max_books=3)
        print(f"   ✅ Collected {len(raw_books)} raw book records")
        
        if raw_books:
            print(f"   📊 Sample raw book:")
            print(f"      Source: {raw_books[0].source}")
            print(f"      URL: {raw_books[0].source_url}")
            print(f"      Content length: {len(raw_books[0].raw_content)} chars")
        
        print("\n🎉 IT Bookstore API Test COMPLETED!")
        return True
        
    except Exception as e:
        print(f"❌ API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_itbookstore_api()