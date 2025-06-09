#!/usr/bin/env python3
"""
Improved debug test with better anti-detection measures
"""

import sys
import os
import logging
import requests
import time
import random
from bs4 import BeautifulSoup
from decimal import Decimal
from datetime import datetime
from typing import Dict, Any, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.utils import safe_float_conversion

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def create_stealthy_session():
    """Create a more realistic session to avoid detection."""
    session = requests.Session()
    
    # More realistic headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,tr;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    })
    return session

def test_with_delays(session: requests.Session, symbol: str) -> Optional[Dict[str, Any]]:
    """Test with realistic delays and retry logic."""
    logger.info(f"🔍 Testing {symbol} with anti-detection measures")
    
    try:
        # Random delay to appear more human
        delay = random.uniform(3, 7)
        logger.debug(f"Waiting {delay:.1f}s before request...")
        time.sleep(delay)
        
        url = f"https://finance.yahoo.com/quote/{symbol}"
        logger.debug(f"Fetching: {url}")
        
        # Add random referer
        headers = {
            'Referer': 'https://finance.yahoo.com/',
            'Sec-Fetch-Site': 'same-origin',
        }
        
        response = session.get(url, headers=headers, timeout=15)
        logger.debug(f"Response: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        if response.status_code != 200:
            logger.error(f"❌ HTTP {response.status_code} for {symbol}")
            return None
        
        # Check if we got redirected or blocked
        if 'robots.txt' in response.url or 'blocked' in response.text.lower():
            logger.warning(f"⚠️ Possible blocking detected for {symbol}")
        
        # Log a snippet of the response to see what we're getting
        response_snippet = response.text[:500]
        logger.debug(f"Response snippet: {response_snippet}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Check for common anti-bot patterns
        if soup.find(string=lambda text: text and 'access denied' in text.lower()):
            logger.warning(f"⚠️ Access denied message detected for {symbol}")
        
        # Try to extract data
        data = extract_realistic_data(soup, symbol)
        
        # Validate data makes sense
        if data and 'price' in data:
            price = safe_float_conversion(data['price'])
            # Basic sanity check - AAPL should be $100-400, GOOGL should be $100-300
            if symbol == 'AAPL' and (price < 50 or price > 500):
                logger.warning(f"⚠️ AAPL price {price} seems unrealistic")
            elif symbol == 'GOOGL' and (price < 50 or price > 400):
                logger.warning(f"⚠️ GOOGL price {price} seems unrealistic")
        
        return data
        
    except Exception as e:
        logger.error(f"❌ Error testing {symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def extract_realistic_data(soup: BeautifulSoup, symbol: str) -> Optional[Dict[str, Any]]:
    """Extract data and check for realistic values."""
    logger.debug(f"Extracting data for {symbol}")
    
    try:
        data = {}
        
        # First, let's see what fin-streamer elements we can find
        fin_streamers = soup.find_all('fin-streamer')
        logger.debug(f"Found {len(fin_streamers)} fin-streamer elements")
        
        for element in fin_streamers[:5]:  # Log first 5
            field = element.get('data-field', 'unknown')
            value = element.get_text().strip()
            logger.debug(f"  fin-streamer[data-field='{field}']: '{value}'")
        
        # Try different extraction methods
        price_selectors = [
            'fin-streamer[data-field="regularMarketPrice"]',
            '[data-field="regularMarketPrice"]',
            '[data-testid="qsp-price"]',
            '.livePrice .value',
        ]
        
        for selector in price_selectors:
            element = soup.select_one(selector)
            if element:
                price_text = element.get_text().strip()
                logger.debug(f"Found price with selector '{selector}': '{price_text}'")
                data['price'] = price_text
                break
        
        # Try to get change
        change_selectors = [
            'fin-streamer[data-field="regularMarketChange"]',
            '[data-field="regularMarketChange"]',
        ]
        
        for selector in change_selectors:
            element = soup.select_one(selector)
            if element:
                change_text = element.get_text().strip()
                logger.debug(f"Found change with selector '{selector}': '{change_text}'")
                data['daily_change_nominal'] = change_text
                break
        
        # Also try to find the page title for validation
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            logger.debug(f"Page title: '{title_text}'")
            if symbol.upper() not in title_text.upper():
                logger.warning(f"⚠️ Symbol {symbol} not found in page title: {title_text}")
        
        # Log some key page elements to understand structure
        h1_elements = soup.find_all('h1')
        for h1 in h1_elements[:3]:
            logger.debug(f"H1 found: '{h1.get_text().strip()}'")
        
        logger.info(f"Extracted data for {symbol}: {data}")
        return data if data else None
        
    except Exception as e:
        logger.error(f"Error extracting data for {symbol}: {e}")
        return None

def validate_data(data: Dict[str, Any], symbol: str) -> bool:
    """Validate that extracted data looks realistic."""
    if not data or 'price' not in data:
        return False
    
    try:
        price = safe_float_conversion(data['price'])
        
        # Basic sanity checks
        if price <= 0:
            logger.warning(f"❌ Invalid price {price} for {symbol}")
            return False
        
        # Symbol-specific checks
        if symbol == 'AAPL':
            if price < 50 or price > 500:
                logger.warning(f"❌ AAPL price {price} outside expected range $50-$500")
                return False
        elif symbol == 'GOOGL':
            if price < 50 or price > 400:
                logger.warning(f"❌ GOOGL price {price} outside expected range $50-$400")
                return False
        
        logger.info(f"✅ Data validation passed for {symbol}: price=${price}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error validating data for {symbol}: {e}")
        return False

def main():
    """Main function."""
    logger.info("🚀 Improved Debug Test for NASDAQ Scraper")
    logger.info("Testing with anti-detection measures...")
    logger.info("=" * 60)
    
    # Create stealthy session
    session = create_stealthy_session()
    test_symbols = ["AAPL", "GOOGL"]
    results = {}
    
    for i, symbol in enumerate(test_symbols):
        logger.info(f"\n{'='*15} Testing {symbol} ({i+1}/{len(test_symbols)}) {'='*15}")
        
        # Test with delays
        result = test_with_delays(session, symbol)
        
        if result:
            # Validate the data
            is_valid = validate_data(result, symbol)
            results[symbol] = {'data': result, 'valid': is_valid}
        else:
            results[symbol] = {'data': None, 'valid': False}
        
        # Delay between requests
        if i < len(test_symbols) - 1:
            delay = random.uniform(5, 10)
            logger.info(f"💤 Waiting {delay:.1f}s before next request...")
            time.sleep(delay)
    
    # Summary
    logger.info(f"\n🎯 FINAL SUMMARY:")
    success_count = 0
    valid_count = 0
    
    for symbol, result in results.items():
        if result['data']:
            if result['valid']:
                logger.info(f"  ✅ {symbol}: SUCCESS (valid data)")
                success_count += 1
                valid_count += 1
            else:
                logger.info(f"  ⚠️  {symbol}: EXTRACTED but invalid data")
                success_count += 1
        else:
            logger.info(f"  ❌ {symbol}: FAILED")
    
    logger.info(f"\nExtraction: {success_count}/{len(test_symbols)} successful")
    logger.info(f"Validation: {valid_count}/{len(test_symbols)} realistic")
    
    if valid_count == 0:
        logger.warning("🚨 No realistic data extracted - Yahoo Finance may be blocking or serving fake data")
        logger.warning("💡 Consider using alternative data sources or rotating IP addresses")
    
    logger.info("🏁 Test completed!")
    
    return valid_count > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)