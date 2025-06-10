#!/usr/bin/env python3
"""Debug script to test Yahoo Finance scraping for specific symbols."""

import sys
import requests
from bs4 import BeautifulSoup
sys.path.append('src')

def debug_yahoo_scrape(symbol):
    """Debug Yahoo Finance scraping for a specific symbol."""
    print(f"\n{'='*60}")
    print(f"DEBUGGING {symbol}")
    print(f"{'='*60}")
    
    url = f"https://finance.yahoo.com/quote/{symbol}"
    print(f"URL: {url}")
    
    # Make request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Length: {len(response.content)} bytes")
        
        if response.status_code != 200:
            print(f"ERROR: Non-200 status code")
            return
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Debug: Show page title
        title = soup.find('title')
        print(f"Page Title: {title.text if title else 'No title found'}")
        
        # Look for price elements
        print(f"\n--- PRICE ELEMENTS ---")
        price_selectors = [
            'fin-streamer[data-field="regularMarketPrice"]',
            '[data-field="regularMarketPrice"]',
            '[data-testid="qsp-price"]'
        ]
        
        for selector in price_selectors:
            elements = soup.select(selector)
            print(f"Selector: {selector}")
            print(f"Found {len(elements)} elements")
            for i, elem in enumerate(elements[:3]):  # Show first 3
                print(f"  [{i}] Text: '{elem.text.strip()}'")
                print(f"  [{i}] Value attr: '{elem.get('value', 'None')}'")
                print(f"  [{i}] Data-value attr: '{elem.get('data-value', 'None')}'")
            print()
        
        # Look for range elements
        print(f"--- RANGE ELEMENTS ---")
        range_selectors = [
            'span[title="Day\'s Range"]',
            'td[data-test="DAYS_RANGE-value"]',
            '[data-test="DAYS_RANGE-value"]'
        ]
        
        for selector in range_selectors:
            elements = soup.select(selector)
            print(f"Selector: {selector}")
            print(f"Found {len(elements)} elements")
            for i, elem in enumerate(elements[:3]):
                print(f"  [{i}] Text: '{elem.text.strip()}'")
                # Try to find next sibling
                next_elem = elem.find_next_sibling()
                if next_elem:
                    print(f"  [{i}] Next sibling: '{next_elem.text.strip()}'")
            print()
        
        # Look for open/close elements
        print(f"--- OPEN/CLOSE ELEMENTS ---")
        open_close_selectors = [
            '[data-field="regularMarketOpen"]',
            '[data-field="regularMarketPreviousClose"]',
            'td[data-test="OPEN-value"]',
            'td[data-test="PREV_CLOSE-value"]'
        ]
        
        for selector in open_close_selectors:
            elements = soup.select(selector)
            print(f"Selector: {selector}")
            print(f"Found {len(elements)} elements")
            for i, elem in enumerate(elements[:2]):
                print(f"  [{i}] Text: '{elem.text.strip()}'")
                print(f"  [{i}] Value attr: '{elem.get('value', 'None')}'")
            print()
        
        # Show some raw HTML around price area (for debugging)
        print(f"--- RAW HTML SAMPLE ---")
        price_area = soup.find('fin-streamer', {'data-field': 'regularMarketPrice'})
        if price_area:
            # Get parent container
            container = price_area.parent.parent if price_area.parent else price_area
            html_sample = str(container)[:1000]  # First 1000 chars
            print(f"HTML around price area (truncated):")
            print(html_sample)
        else:
            print("No price area found - let's see general structure")
            body = soup.find('body')
            if body:
                print("Body structure (first 500 chars):")
                print(str(body)[:500])
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run debug tests for Apple and Google."""
    symbols = ['AAPL', 'GOOGL']
    
    for symbol in symbols:
        debug_yahoo_scrape(symbol)
        print(f"\n{'*'*60}\n")

if __name__ == '__main__':
    main()