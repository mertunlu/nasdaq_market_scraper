"""Main scraping logic for NASDAQ-100 stock data."""

import time
import logging
import requests
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from src.config import config
from src.exceptions import (
    NetworkError, DataValidationError, ParsingError, 
    RateLimitError, TimeoutError, SymbolNotFoundError
)
from src.models import StockData, ScrapingResult, BatchResult
from src.utils import (
    get_request_headers, parse_financial_value, parse_volume,
    validate_stock_data, RateLimiter, performance_timer,
    safe_float_conversion, retry_with_backoff
)


class YahooFinanceScraper:
    """Scraper for Yahoo Finance stock data."""
    
    def __init__(self, rate_limiter: RateLimiter = None):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = self._create_session()
        self.base_url = config.YAHOO_FINANCE_BASE_URL
        
        # Statistics tracking
        self.stats = {
            'requests_made': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'rate_limit_hits': 0,
            'timeout_errors': 0,
            'parsing_errors': 0
        }
        
        self.logger.info("Yahoo Finance scraper initialized")
    
    def _create_session(self) -> requests.Session:
        """Create and configure requests session."""
        session = requests.Session()
        session.headers.update(get_request_headers())
        
        # Configure retries at session level
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    @performance_timer
    def scrape_symbol(self, symbol: str) -> Optional[StockData]:
        """Scrape stock data for a single symbol."""
        if not symbol or not isinstance(symbol, str):
            raise ValueError(f"Invalid symbol: {symbol}")
        
        symbol = symbol.upper().strip()
        url = urljoin(self.base_url, symbol)
        
        try:
            # Apply rate limiting
            wait_time = self.rate_limiter.wait_if_needed()
            if wait_time > 0:
                self.stats['rate_limit_hits'] += 1
                self.logger.debug(f"Rate limited, waited {wait_time:.2f}s for {symbol}")
            
            # Add request delay
            time.sleep(config.REQUEST_DELAY)
            
            # Make request
            self.logger.debug(f"Scraping {symbol} from {url}")
            response = self._make_request(url)
            self.stats['requests_made'] += 1
            
            # Parse data
            stock_data = self._parse_response(response, symbol)
            
            if stock_data:
                self.stats['successful_scrapes'] += 1
                self.logger.debug(f"Successfully scraped {symbol}: ${stock_data.price}")
                return stock_data
            else:
                self.stats['failed_scrapes'] += 1
                return None
                
        except (NetworkError, TimeoutError, RateLimitError) as e:
            self.stats['failed_scrapes'] += 1
            self.logger.warning(f"Network error scraping {symbol}: {e}")
            raise
        except ParsingError as e:
            self.stats['parsing_errors'] += 1
            self.stats['failed_scrapes'] += 1
            self.logger.warning(f"Parsing error for {symbol}: {e}")
            raise
        except Exception as e:
            self.stats['failed_scrapes'] += 1
            self.logger.error(f"Unexpected error scraping {symbol}: {e}")
            raise NetworkError(f"Unexpected error: {e}", symbol)
    
    @retry_with_backoff(max_retries=3)
    def _make_request(self, url: str) -> requests.Response:
        """Make HTTP request with error handling."""
        try:
            # Rotate user agent occasionally
            if self.stats['requests_made'] % 10 == 0:
                self.session.headers.update(get_request_headers())
            
            response = self.session.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            # Check for rate limiting
            if response.status_code == 429:
                raise RateLimitError(f"Rate limited by Yahoo Finance: {response.status_code}")
            
            # Check for other HTTP errors
            if response.status_code == 404:
                raise SymbolNotFoundError(f"Symbol not found: {response.status_code}")
            elif response.status_code >= 500:
                raise NetworkError(f"Server error: {response.status_code}")
            elif response.status_code >= 400:
                raise NetworkError(f"Client error: {response.status_code}")
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            self.stats['timeout_errors'] += 1
            raise TimeoutError(f"Request timeout for {url}")
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {e}")
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request error: {e}")
    
    def _parse_response(self, response: requests.Response, symbol: str) -> Optional[StockData]:
        """Parse HTML response to extract stock data."""
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Yahoo Finance uses various selectors for different data points
            data = {}
            
            # Primary selectors for stock data
            selectors = {
                'price': [
                    'fin-streamer[data-field="regularMarketPrice"]',
                    '[data-field="regularMarketPrice"]',
                    '[data-testid="qsp-price"]',
                    '.Trsdu\\(0\\.3s\\) .Fw\\(b\\).Fz\\(36px\\)'
                ],
                'change': [
                    'fin-streamer[data-field="regularMarketChange"]',
                    '[data-field="regularMarketChange"]',
                    '[data-testid="qsp-price-change"]'
                ],
                'change_percent': [
                    'fin-streamer[data-field="regularMarketChangePercent"]',
                    '[data-field="regularMarketChangePercent"]',
                    '[data-testid="qsp-price-change-percent"]'
                ],
                'volume': [
                    'fin-streamer[data-field="regularMarketVolume"]',
                    '[data-field="regularMarketVolume"]',
                    'td[data-test="VOLUME-value"]'
                ],
                'open': [
                    'fin-streamer[data-field="regularMarketOpen"]',
                    '[data-field="regularMarketOpen"]',
                    'td[data-test="OPEN-value"]'
                ],
                'previous_close': [
                    'fin-streamer[data-field="regularMarketPreviousClose"]',
                    '[data-field="regularMarketPreviousClose"]',
                    'td[data-test="PREV_CLOSE-value"]'
                ]
            }
            
            # Extract data using multiple selector strategies
            for field, selector_list in selectors.items():
                value = None
                for selector in selector_list:
                    try:
                        element = soup.select_one(selector)
                        if element:
                            # Try different attribute sources
                            value = (element.get('value') or 
                                   element.get('data-value') or 
                                   element.text.strip())
                            if value:
                                break
                    except Exception as e:
                        self.logger.debug(f"Selector {selector} failed for {field}: {e}")
                        continue
                
                if value:
                    data[field] = value
                else:
                    self.logger.warning(f"Could not find {field} for {symbol}")
            
            # Special handling for day range (high/low combined)
            day_range = self._extract_day_range(soup)
            if day_range:
                data['high'], data['low'] = day_range
            else:
                self.logger.warning(f"Could not find day range for {symbol}")
            
            # Validate we have minimum required data
            required_fields = ['price']
            if not all(field in data for field in required_fields):
                missing = [f for f in required_fields if f not in data]
                raise ParsingError(f"Missing required fields for {symbol}: {missing}")
            
            # Parse and validate the extracted data
            parsed_data = self._parse_extracted_data(data, symbol)
            
            if not parsed_data:
                raise ParsingError(f"Failed to parse data for {symbol}")
            
            # Create StockData object
            stock_data = StockData(
                symbol=symbol,
                price=parsed_data['price'],
                daily_change_percent=parsed_data.get('daily_change_percent', Decimal('0')),
                daily_change_nominal=parsed_data.get('daily_change_nominal', Decimal('0')),
                volume=parsed_data.get('volume', 0),
                high=parsed_data.get('high', parsed_data['price']),
                low=parsed_data.get('low', parsed_data['price']),
                open=parsed_data.get('open', parsed_data['price']),
                previous_close=parsed_data.get('previous_close', parsed_data['price']),
                last_updated=datetime.utcnow().isoformat() + 'Z',
                market='NASDAQ'
            )
            
            # Validate the stock data
            if not stock_data.validate():
                raise DataValidationError(f"Invalid stock data for {symbol}")
            
            return stock_data
            
        except ParsingError:
            raise
        except DataValidationError:
            raise
        except Exception as e:
            raise ParsingError(f"Error parsing response for {symbol}: {e}")
    
    def _extract_day_range(self, soup: BeautifulSoup) -> Optional[tuple]:
        """Extract day range (high/low) from various formats."""
        try:
            # Strategy 1: Look for Day's Range label + value
            range_selectors = [
                # Look for span with Day's Range title and get next sibling
                'span[title="Day\'s Range"] + span',
                'span[title="Day\'s Range"]',
                # Look for the data-test attribute
                'td[data-test="DAYS_RANGE-value"]',
                '[data-test="DAYS_RANGE-value"]',
                # Generic patterns
                '.Ta\\(end\\).Fw\\(600\\).Lh\\(14px\\)'
            ]
            
            for selector in range_selectors:
                try:
                    element = soup.select_one(selector)
                    if element:
                        # If this is the label, try to find the value
                        if 'Day\'s Range' in element.get('title', ''):
                            # Look for next sibling or parent's next element
                            value_element = element.find_next_sibling() or element.parent.find_next_sibling()
                            if value_element:
                                range_text = value_element.text.strip()
                            else:
                                continue
                        else:
                            range_text = element.text.strip()
                        
                        # Parse "150.25 - 152.10" format
                        if ' - ' in range_text:
                            parts = range_text.split(' - ')
                            if len(parts) == 2:
                                low_val = parse_financial_value(parts[0])
                                high_val = parse_financial_value(parts[1])
                                if low_val and high_val:
                                    return (high_val, low_val)
                except Exception as e:
                    self.logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Strategy 2: Look for individual high/low elements
            high_selectors = [
                'fin-streamer[data-field="regularMarketDayHigh"]',
                '[data-field="regularMarketDayHigh"]',
                'td[data-test="DAYS_RANGE-value"] span:first-child'
            ]
            
            low_selectors = [
                'fin-streamer[data-field="regularMarketDayLow"]',
                '[data-field="regularMarketDayLow"]',
                'td[data-test="DAYS_RANGE-value"] span:last-child'
            ]
            
            high_val = None
            low_val = None
            
            for selector in high_selectors:
                element = soup.select_one(selector)
                if element:
                    value = element.get('value') or element.text.strip()
                    high_val = parse_financial_value(value)
                    if high_val:
                        break
            
            for selector in low_selectors:
                element = soup.select_one(selector)
                if element:
                    value = element.get('value') or element.text.strip()
                    low_val = parse_financial_value(value)
                    if low_val:
                        break
            
            if high_val and low_val:
                return (high_val, low_val)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting day range: {e}")
            return None
    
    def _parse_extracted_data(self, data: Dict[str, str], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse and convert extracted string data to appropriate types."""
        try:
            parsed = {}
            
            # Parse price (required)
            if 'price' in data:
                price = parse_financial_value(data['price'])
                if price and price > 0:
                    parsed['price'] = price
                else:
                    self.logger.error(f"Invalid price for {symbol}: {data['price']}")
                    return None
            else:
                return None
            
            # Parse daily change nominal
            if 'change' in data:
                change = parse_financial_value(data['change'])
                if change is not None:
                    parsed['daily_change_nominal'] = change
            
            # Parse daily change percentage
            if 'change_percent' in data:
                change_pct = parse_financial_value(data['change_percent'])
                if change_pct is not None:
                    parsed['daily_change_percent'] = change_pct
            
            # Parse volume
            if 'volume' in data:
                volume = parse_volume(data['volume'])
                if volume is not None and volume >= 0:
                    parsed['volume'] = volume
            
            # Parse open
            if 'open' in data:
                open_price = parse_financial_value(data['open'])
                if open_price and open_price > 0:
                    parsed['open'] = open_price
            
            # Parse previous close
            if 'previous_close' in data:
                prev_close = parse_financial_value(data['previous_close'])
                if prev_close and prev_close > 0:
                    parsed['previous_close'] = prev_close
            
            # Parse high
            if 'high' in data:
                high = parse_financial_value(data['high'])
                if high and high > 0:
                    parsed['high'] = high
            
            # Parse low
            if 'low' in data:
                low = parse_financial_value(data['low'])
                if low and low > 0:
                    parsed['low'] = low
            
            # Set defaults for missing optional fields
            if 'daily_change_percent' not in parsed:
                parsed['daily_change_percent'] = Decimal('0')
            if 'daily_change_nominal' not in parsed:
                parsed['daily_change_nominal'] = Decimal('0')
            if 'volume' not in parsed:
                parsed['volume'] = 0
            if 'high' not in parsed:
                parsed['high'] = parsed['price']
            if 'low' not in parsed:
                parsed['low'] = parsed['price']
            if 'open' not in parsed:
                parsed['open'] = parsed['price']
            if 'previous_close' not in parsed:
                parsed['previous_close'] = parsed['price']
            
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing extracted data for {symbol}: {e}")
            return None
    
    def scrape_batch(self, symbols: List[str]) -> BatchResult:
        """Scrape multiple symbols and return batch results."""
        start_time = datetime.utcnow()
        results = []
        successful = 0
        failed = 0
        
        self.logger.info(f"Starting batch scrape of {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols):
            try:
                # Progress logging
                if (i + 1) % 10 == 0:
                    self.logger.info(f"Progress: {i + 1}/{len(symbols)} symbols processed")
                
                stock_data = self.scrape_symbol(symbol)
                if stock_data:
                    result = ScrapingResult(symbol=symbol, success=True, data=stock_data)
                    successful += 1
                else:
                    result = ScrapingResult(symbol=symbol, success=False, error="No data returned")
                    failed += 1
                
                results.append(result)
                
            except Exception as e:
                result = ScrapingResult(symbol=symbol, success=False, error=str(e))
                results.append(result)
                failed += 1
                self.logger.warning(f"Failed to scrape {symbol}: {e}")
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        batch_result = BatchResult(
            total_symbols=len(symbols),
            successful=successful,
            failed=failed,
            results=results,
            start_time=start_time.isoformat() + 'Z',
            end_time=end_time.isoformat() + 'Z',
            duration_seconds=duration
        )
        
        self.logger.info(
            f"Batch scrape completed: {successful}/{len(symbols)} successful "
            f"({batch_result.success_rate:.1f}%) in {duration:.1f}s"
        )
        
        return batch_result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraper statistics."""
        total_requests = self.stats['requests_made']
        success_rate = (
            (self.stats['successful_scrapes'] / total_requests * 100) 
            if total_requests > 0 else 0
        )
        
        return {
            **self.stats,
            'success_rate_percent': round(success_rate, 2),
            'current_rate_limit': self.rate_limiter.get_current_rate(),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    
    def reset_stats(self):
        """Reset scraper statistics."""
        self.stats = {
            'requests_made': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'rate_limit_hits': 0,
            'timeout_errors': 0,
            'parsing_errors': 0
        }
        self.logger.info("Scraper statistics reset")
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'session'):
            self.session.close()
        self.logger.info("Scraper session closed")


class NasdaqScraper:
    """Main NASDAQ-100 scraper orchestrator."""
    
    def __init__(self, symbols_file: str = None, debug: bool = None):
        self.debug = debug if debug is not None else config.DEBUG
        self.logger = logging.getLogger(__name__)
        
        # Load symbols
        try:
            from src.utils import load_nasdaq_symbols
            self.symbols = load_nasdaq_symbols(symbols_file)
            self.logger.info(f"Loaded {len(self.symbols)} NASDAQ symbols")
        except Exception as e:
            self.logger.error(f"Failed to load symbols: {e}")
            raise
        
        # Initialize components
        self.rate_limiter = RateLimiter()
        self.scraper = YahooFinanceScraper(self.rate_limiter)
        
        # Limit symbols in debug mode
        if self.debug and len(self.symbols) > config.MAX_SYMBOLS_PER_BATCH:
            self.symbols = self.symbols[:config.MAX_SYMBOLS_PER_BATCH]
            self.logger.info(f"Debug mode: limited to {len(self.symbols)} symbols")
    
    def scrape_all(self) -> BatchResult:
        """Scrape all NASDAQ-100 symbols."""
        return self.scraper.scrape_batch(self.symbols)
    
    def get_symbols(self) -> List[str]:
        """Get list of symbols being tracked."""
        return self.symbols.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive scraper statistics."""
        return self.scraper.get_stats()
    
    def close(self):
        """Clean up resources."""
        self.scraper.close()