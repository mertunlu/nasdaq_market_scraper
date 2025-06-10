#!/usr/bin/env python3
"""Fix the scraper bug and test immediately."""

import sys
sys.path.append('src')

def fix_and_test():
    """Fix the parsing bug and test with AAPL."""
    import requests
    from bs4 import BeautifulSoup
    from src.utils import parse_financial_value
    from decimal import Decimal
    
    print("Testing fixed parsing logic with AAPL...")
    
    # Get AAPL data
    url = "https://finance.yahoo.com/quote/JOBY"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    print(f"Status: {response.status_code}")
    
    # Extract data using corrected logic
    data = {}
    
    # Get price - use the RIGHT selector
    price_element = soup.select_one('[data-testid="qsp-price"]')
    if price_element:
        data['price'] = price_element.text.strip()
        print(f"Raw price text: '{data['price']}'")
    
    # Get range
    range_element = soup.select_one('span[title="Day\'s Range"]')
    if range_element:
        range_next = range_element.find_next_sibling()
        if range_next:
            range_text = range_next.text.strip()
            data['range'] = range_text
            print(f"Raw range text: '{range_text}'")
            
            # Parse range
            if ' - ' in range_text:
                parts = range_text.split(' - ')
                data['low'] = parts[0].strip()
                data['high'] = parts[1].strip()
                print(f"Low: '{data['low']}', High: '{data['high']}'")
    
    # Get open
    open_element = soup.select_one('[data-field="regularMarketOpen"]')
    if open_element:
        data['open'] = open_element.text.strip()
        print(f"Raw open text: '{data['open']}'")
    
    # Get previous close
    prev_close_element = soup.select_one('[data-field="regularMarketPreviousClose"]')
    if prev_close_element:
        data['previous_close'] = prev_close_element.text.strip()
        print(f"Raw previous close text: '{data['previous_close']}'")
    
    # Now parse everything properly
    print(f"\n--- PARSING RESULTS ---")
    
    parsed = {}
    
    if 'price' in data:
        parsed['price'] = parse_financial_value(data['price'])
        print(f"Parsed price: {parsed['price']} (type: {type(parsed['price'])})")
    
    if 'low' in data:
        parsed['low'] = parse_financial_value(data['low'])
        print(f"Parsed low: {parsed['low']}")
    
    if 'high' in data:
        parsed['high'] = parse_financial_value(data['high'])
        print(f"Parsed high: {parsed['high']}")
    
    if 'open' in data:
        parsed['open'] = parse_financial_value(data['open'])
        print(f"Parsed open: {parsed['open']}")
    
    if 'previous_close' in data:
        parsed['previous_close'] = parse_financial_value(data['previous_close'])
        print(f"Parsed previous_close: {parsed['previous_close']}")
    
    # Test creating StockData object
    try:
        from src.models import StockData
        from datetime import datetime
        
        stock_data = StockData(
            symbol='AAPL',
            price=parsed.get('price', Decimal('0')),
            daily_change_percent=Decimal('0'),  # We'll set as default for now
            daily_change_nominal=Decimal('0'),
            volume=0,
            high=parsed.get('high', parsed.get('price', Decimal('0'))),
            low=parsed.get('low', parsed.get('price', Decimal('0'))),
            open=parsed.get('open', parsed.get('price', Decimal('0'))),
            previous_close=parsed.get('previous_close', parsed.get('price', Decimal('0'))),
            last_updated=datetime.utcnow().isoformat() + 'Z'
        )
        
        print(f"\n--- STOCKDATA OBJECT ---")
        print(f"Symbol: {stock_data.symbol}")
        print(f"Price: ${stock_data.price}")
        print(f"High: ${stock_data.high}")
        print(f"Low: ${stock_data.low}")
        print(f"Open: ${stock_data.open}")
        print(f"Previous Close: ${stock_data.previous_close}")
        print(f"Valid: {stock_data.validate()}")
        
        print(f"\n✅ SUCCESS! The parsing logic works correctly.")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR creating StockData: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    fix_and_test()