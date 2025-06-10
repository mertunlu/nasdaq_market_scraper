#!/usr/bin/env python3
"""Test batch scraping."""

import sys
sys.path.append('src')

def test_batch():
    from src.scraper import YahooFinanceScraper
    
    scraper = YahooFinanceScraper()
    symbols = ['AAPL', 'GOOGL']  # Just 2 symbols
    
    print(f"Testing batch scrape of {symbols}...")
    try:
        result = scraper.scrape_batch(symbols)
        print(f"✅ Batch completed: {result.successful}/{result.total_symbols} successful")
        
        for r in result.results:
            if r.success:
                print(f"✅ {r.symbol}: ${r.data.price}")
            else:
                print(f"❌ {r.symbol}: {r.error}")
                
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()

if __name__ == '__main__':
    test_batch()