#!/usr/bin/env python3
"""
Inspect the actual HTML structure to find authors
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

from ingestion.books_toscrape import BooksToScrapeScraper
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)

def inspect_html_structure():
    """Inspect the HTML structure to find where authors are located"""
    scraper = BooksToScrapeScraper()
    
    categories = scraper.get_all_categories()
    if not categories:
        print("❌ No categories found")
        return
    
    category_url = categories[0]['url']
    raw_pages = scraper.scrape_category(category_url, max_pages=1)
    
    book_pages = [
        page for page in raw_pages
        if '/catalogue/' in page.source_url and '/category/' not in page.source_url
    ]
    
    if not book_pages:
        print("❌ No book pages found")
        return
    
    book_page = book_pages[0]
    print(f"🔍 Inspecting HTML structure of: {book_page.source_url}")
    print("=" * 80)
    
    soup = BeautifulSoup(book_page.raw_content, 'html.parser')
    
    # 1. Check the entire product table
    print("1. PRODUCT INFORMATION TABLE:")
    print("-" * 40)
    table = soup.find('table', class_='table table-striped')
    if table:
        rows = table.find_all('tr')
        for i, row in enumerate(rows):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                print(f"   Row {i+1}: {th.text.strip()} → {td.text.strip()}")
    else:
        print("   Table not found!")
    
    # 2. Check for any elements containing "author" or "by"
    print("\n2. SEARCHING FOR AUTHOR-RELATED ELEMENTS:")
    print("-" * 40)
    
    # Search for text containing "author"
    author_elements = soup.find_all(string=lambda text: text and 'author' in text.lower())
    for elem in author_elements[:5]:  # Show first 5
        parent = elem.parent
        print(f"   Found 'author' in: {parent.name} with text: {elem.strip()}")
    
    # Search for elements that might contain authors
    possible_selectors = [
        '.product_main .author',
        '.product_main .by',
        '.product_main small',
        'p.author',
        '.author',
    ]
    
    for selector in possible_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"   Selector '{selector}' found: {[elem.text.strip() for elem in elements]}")
    
    # 3. Check the product main section
    print("\n3. PRODUCT MAIN SECTION:")
    print("-" * 40)
    product_main = soup.select_one('.product_main')
    if product_main:
        print("   Children elements:")
        for child in product_main.children:
            if hasattr(child, 'name') and child.name:
                print(f"     {child.name}: {child.text.strip()[:100]}")

if __name__ == "__main__":
    inspect_html_structure()