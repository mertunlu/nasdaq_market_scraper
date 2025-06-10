#!/usr/bin/env python3
"""Debug the main application hang issue."""

import sys
sys.path.append('src')

def debug_nasdaq_scraper():
    """Debug the NasdaqScraper initialization and symbol loading."""
    print("=== Debugging NasdaqScraper ===")
    
    from src.scraper import NasdaqScraper
    from src.config import config
    
    print(f"Debug mode: {config.DEBUG}")
    print(f"Max symbols per batch: {config.MAX_SYMBOLS_PER_BATCH}")
    
    # Initialize scraper like the main app does
    print("1. Initializing NasdaqScraper...")
    try:
        scraper = NasdaqScraper(debug=config.DEBUG)
        print(f"   ✅ Scraper initialized")
        
        # Check what symbols it loaded
        symbols = scraper.get_symbols()
        print(f"2. Symbols loaded: {len(symbols)}")
        print(f"   Symbols: {symbols}")
        
        # Try to scrape them (this is where it might hang)
        print("3. Testing scrape_all() method...")
        print("   (This is where the main app gets stuck)")
        
        # Let's try with just first 2 symbols to test
        print("4. Testing with first 2 symbols only...")
        test_symbols = symbols[:2]
        print(f"   Testing: {test_symbols}")
        
        # Use the underlying scraper directly
        batch_result = scraper.scraper.scrape_batch(test_symbols)
        print(f"   ✅ Batch result: {batch_result.successful}/{batch_result.total_symbols}")
        
        # Now try the full scrape_all method
        print("5. Now trying full scrape_all()...")
        batch_result = scraper.scrape_all()
        print(f"   ✅ Full scrape result: {batch_result.successful}/{batch_result.total_symbols}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_nasdaq_scraper()