"""Main scraping logic for NASDAQ-100 stock data - FIXED VERSION with Real-time Data."""

import time
import logging
import requests
from datetime import datetime, UTC
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
    """Scraper for Yahoo Finance stock data with real-time market state handling."""
    
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
            'parsing_errors': 0,
            'market_hours_data': 0,
            'after_hours_data': 0,
            'pre_market_data': 0
        }
        
        self.logger.info("Yahoo Finance scraper initialized with real-time data support")
    
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
        """Scrape stock data for a single symbol with market state awareness."""
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
            
            # Parse data with market state awareness
            stock_data = self._parse_response_with_market_state(response, symbol)
            
            if stock_data:
                self.stats['successful_scrapes'] += 1
                self.logger.debug(f"Successfully scraped {symbol}: ${stock_data.price}")
                return stock_data
            else:
                self.stats['failed_scrapes'] += 1
                return None
                
        except (NetworkError, DataValidationError, ParsingError,
                RateLimitError, TimeoutError, SymbolNotFoundError) as e:
            self.stats['failed_scrapes'] += 1
            self.logger.error(f"Failed to scrape {symbol}: {e}")
            raise e
    
    def _parse_response_with_market_state(self, response: requests.Response, symbol: str) -> Optional[StockData]:
        """Parse HTML response with awareness of market state (regular/pre/post market)."""
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Strategy: Try to get the most current data available based on market state
            # Priority: Post-market > Pre-market > Regular market > Previous close
            
            market_state = self._detect_market_state(soup)
            self.logger.debug(f"Detected market state for {symbol}: {market_state}")
            
            # Extract data based on market state
            if market_state == 'post_market':
                data = self._extract_post_market_data(soup, symbol)
                self.stats['after_hours_data'] += 1
            elif market_state == 'pre_market':
                data = self._extract_pre_market_data(soup, symbol)
                self.stats['pre_market_data'] += 1
            elif market_state == 'regular':
                data = self._extract_regular_market_data(soup, symbol)
                self.stats['market_hours_data'] += 1
            else:
                # Fallback to any available data
                data = self._extract_fallback_data(soup, symbol)
                self.logger.debug(f"Using fallback data extraction for {symbol}")
            
            if not data or 'price' not in data:
                raise ParsingError(f"No valid price data found for {symbol}")
            
            # Parse and validate the extracted data
            parsed_data = self._parse_extracted_data(data, symbol)
            
            if not parsed_data:
                raise ParsingError(f"Failed to parse data for {symbol}")
            
            # Create StockData object
            stock_data = StockData(
                symbol=symbol,
                price=parsed_data['price'],
                daily_change_percent=parsed_data['daily_change_percent'],
                daily_change_nominal=parsed_data['daily_change_nominal'],
                volume=parsed_data['volume'],
                high=parsed_data['high'],
                low=parsed_data['low'],
                open=parsed_data['open'],
                previous_close=parsed_data['previous_close'],
                last_updated=datetime.now(UTC).isoformat(),
                market='NASDAQ'
            )
            
            # Validate the stock data
            if not stock_data.validate():
                raise DataValidationError(f"Invalid stock data for {symbol}")
            
            self.logger.info(f"✅ {symbol}: ${stock_data.price} ({market_state}) Change: {stock_data.daily_change_percent}%")
            return stock_data
            
        except ParsingError:
            raise
        except DataValidationError:
            raise
        except Exception as e:
            raise ParsingError(f"Error parsing response for {symbol}: {e}")
    
    def _detect_market_state(self, soup: BeautifulSoup) -> str:
        """Detect current market state from page indicators."""
        # Look for market state indicators
        market_indicators = [
            # Post-market indicators
            ('[data-field="postMarketPrice"]', 'post_market'),
            ('span:contains("After Hours")', 'post_market'),
            ('span:contains("Post-Market")', 'post_market'),
            
            # Pre-market indicators  
            ('[data-field="preMarketPrice"]', 'pre_market'),
            ('span:contains("Pre-Market")', 'pre_market'),
            
            # Regular market indicators
            ('[data-field="regularMarketPrice"]', 'regular'),
        ]
        
        for selector, state in market_indicators:
            try:
                if ':contains(' in selector:
                    # Handle text-based selectors differently
                    text_selector = selector.split(':contains(')[0]
                    search_text = selector.split(':contains(')[1].rstrip(')')
                    elements = soup.select(text_selector)
                    if any(search_text.strip('"') in elem.get_text() for elem in elements):
                        return state
                else:
                    if soup.select_one(selector):
                        return state
            except Exception:
                continue
        
        return 'regular'  # Default fallback
    
    def _extract_post_market_data(self, soup: BeautifulSoup, symbol: str) -> Dict[str, Any]:
        """Extract post-market trading data."""
        data = {}
        
        # Post-market specific selectors (also use testid if available)
        selectors = {
            'price': [
                'fin-streamer[data-field="postMarketPrice"]',
                '[data-field="postMarketPrice"]',
                '[data-testid="qsp-price"]',  # Fallback to main price
            ],
            'change': [
                'fin-streamer[data-field="postMarketChange"]',
                '[data-field="postMarketChange"]',
                '[data-testid="qsp-price-change"]',  # Fallback
            ],
            'change_percent': [
                'fin-streamer[data-field="postMarketChangePercent"]',
                '[data-field="postMarketChangePercent"]',
                '[data-testid="qsp-price-change-percent"]',  # Fallback
            ],
            'volume': [
                'fin-streamer[data-field="postMarketVolume"]',
                'fin-streamer[data-field="regularMarketVolume"]',  # Fallback to regular volume
            ]
        }
        
        # Extract post-market data
        for field, selector_list in selectors.items():
            value = self._extract_value_from_selectors(soup, selector_list)
            if value:
                data[field] = value
        
        # Get regular market data for missing fields
        regular_data = self._extract_regular_market_data(soup, symbol)
        for key, value in regular_data.items():
            if key not in data:
                data[key] = value
        
        self.logger.debug(f"Post-market data for {symbol}: {data}")
        return data
    
    def _extract_pre_market_data(self, soup: BeautifulSoup, symbol: str) -> Dict[str, Any]:
        """Extract pre-market trading data."""
        data = {}
        
        # Pre-market specific selectors (also use testid if available)
        selectors = {
            'price': [
                'fin-streamer[data-field="preMarketPrice"]',
                '[data-field="preMarketPrice"]',
                '[data-testid="qsp-price"]',  # Fallback to main price
            ],
            'change': [
                'fin-streamer[data-field="preMarketChange"]',
                '[data-field="preMarketChange"]',
                '[data-testid="qsp-price-change"]',  # Fallback
            ],
            'change_percent': [
                'fin-streamer[data-field="preMarketChangePercent"]',
                '[data-field="preMarketChangePercent"]',
                '[data-testid="qsp-price-change-percent"]',  # Fallback
            ]
        }
        
        # Extract pre-market data
        for field, selector_list in selectors.items():
            value = self._extract_value_from_selectors(soup, selector_list)
            if value:
                data[field] = value
        
        # Get regular market data for missing fields
        regular_data = self._extract_regular_market_data(soup, symbol)
        for key, value in regular_data.items():
            if key not in data:
                data[key] = value
        
        self.logger.debug(f"Pre-market data for {symbol}: {data}")
        return data
    
    def _extract_regular_market_data(self, soup: BeautifulSoup, symbol: str) -> Dict[str, Any]:
        """Extract regular market trading data."""
        data = {}
        
        # FIXED: Prioritize the working data-testid selectors
        selectors = {
            'price': [
                '[data-testid="qsp-price"] span',          # Most reliable current price
                '[data-testid="qsp-price"]',
                'fin-streamer[data-field="regularMarketPrice"]',
                '[data-field="regularMarketPrice"]',
                '.D\\(ib\\).Mend\\(20px\\) .Trsdu\\(0\\.3s\\).Fw\\(b\\).Fz\\(36px\\)',
            ],
            'change': [
                '[data-testid="qsp-price-change"]',        # PRIMARY - Working selector
                'fin-streamer[data-field="regularMarketChange"]',
                '[data-field="regularMarketChange"]',
            ],
            'change_percent': [
                '[data-testid="qsp-price-change-percent"]', # PRIMARY - Working selector
                'fin-streamer[data-field="regularMarketChangePercent"]',
                '[data-field="regularMarketChangePercent"]',
            ],
            'volume': [
                'fin-streamer[data-field="regularMarketVolume"]',
                '[data-field="regularMarketVolume"]',
                'td[data-test="VOLUME-value"]',
            ],
            'open': [
                'fin-streamer[data-field="regularMarketOpen"]',
                '[data-field="regularMarketOpen"]',
                'td[data-test="OPEN-value"]',
            ],
            'previous_close': [
                'fin-streamer[data-field="regularMarketPreviousClose"]',
                '[data-field="regularMarketPreviousClose"]',
                'td[data-test="PREV_CLOSE-value"]',
            ]
        }
        
        # Extract data using multiple selector strategies
        for field, selector_list in selectors.items():
            value = self._extract_value_from_selectors(soup, selector_list)
            if value:
                data[field] = value
        
        # Extract day range (high/low)
        day_range = self._extract_day_range(soup)
        if day_range:
            data['high'], data['low'] = day_range
        
        self.logger.debug(f"Regular market data for {symbol}: {data}")
        return data
    
    def _extract_fallback_data(self, soup: BeautifulSoup, symbol: str) -> Dict[str, Any]:
        """Fallback data extraction when market state is unclear."""
        # PRIORITY: Use the working testid selectors first
        all_price_selectors = [
            # Most current price indicators (PRIORITY ORDER)
            '[data-testid="qsp-price"] span',
            '[data-testid="qsp-price"]',
            
            # Market-specific prices
            'fin-streamer[data-field="postMarketPrice"]',
            'fin-streamer[data-field="preMarketPrice"]', 
            'fin-streamer[data-field="regularMarketPrice"]',
            '[data-field="postMarketPrice"]',
            '[data-field="preMarketPrice"]',
            '[data-field="regularMarketPrice"]',
            
            # CSS fallbacks
            '.D\\(ib\\).Mend\\(20px\\) .Trsdu\\(0\\.3s\\).Fw\\(b\\).Fz\\(36px\\)',
            '.Fw\\(b\\).Fz\\(36px\\)',
        ]
        
        data = {}
        
        # Try to get the most current price
        price = self._extract_value_from_selectors(soup, all_price_selectors)
        if price:
            data['price'] = price
        
        # Get other regular market data as fallback
        regular_data = self._extract_regular_market_data(soup, symbol)
        for key, value in regular_data.items():
            if key not in data:
                data[key] = value
        
        return data
    
    def _extract_value_from_selectors(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Try multiple selectors to extract a value."""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    # Try different attribute sources
                    value = (element.get('value') or 
                           element.get('data-value') or 
                           element.text.strip())
                    if value and value.strip():
                        # Clean the value
                        cleaned_value = value.strip().replace('\n', '').replace('\t', '')
                        if cleaned_value and cleaned_value not in ['--', 'N/A', '']:
                            return cleaned_value
            except Exception as e:
                self.logger.debug(f"Selector {selector} failed: {e}")
                continue
        return None
    
    def _extract_day_range(self, soup: BeautifulSoup) -> Optional[tuple]:
        """Extract day range (high/low) from various formats."""
        try:
            # Multiple strategies for day range
            range_selectors = [
                'td[data-test="DAYS_RANGE-value"]',
                '[data-test="DAYS_RANGE-value"]',
                'fin-streamer[data-field="regularMarketDayRange"]',
                '[data-field="regularMarketDayRange"]',
            ]
            
            for selector in range_selectors:
                try:
                    element = soup.select_one(selector)
                    if element:
                        range_text = element.text.strip()
                        
                        # Parse "150.25 - 152.10" format
                        if ' - ' in range_text:
                            parts = range_text.split(' - ')
                            if len(parts) == 2:
                                low_val = parse_financial_value(parts[0].strip())
                                high_val = parse_financial_value(parts[1].strip())
                                if low_val and high_val:
                                    return (high_val, low_val)
                except Exception as e:
                    self.logger.debug(f"Error with range selector {selector}: {e}")
                    continue
            
            # Fallback: Try individual high/low elements
            high_selectors = [
                'fin-streamer[data-field="regularMarketDayHigh"]',
                '[data-field="regularMarketDayHigh"]'
            ]
            
            low_selectors = [
                'fin-streamer[data-field="regularMarketDayLow"]',
                '[data-field="regularMarketDayLow"]'
            ]
            
            high_val = self._extract_value_from_selectors(soup, high_selectors)
            low_val = self._extract_value_from_selectors(soup, low_selectors)
            
            if high_val and low_val:
                high_parsed = parse_financial_value(high_val)
                low_parsed = parse_financial_value(low_val)
                if high_parsed and low_parsed:
                    return (high_parsed, low_parsed)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting day range: {e}")
            return None
    
    def _calculate_nominal_change(self, change_percent: Decimal, previous_close: Decimal) -> Decimal:
        """Calculate nominal change from percentage and previous close."""
        try:
            if previous_close and change_percent:
                nominal_change = (change_percent * previous_close) / 100
                return nominal_change
            return Decimal('0')
        except Exception:
            return Decimal('0')
    
    def _parse_extracted_data(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse and convert extracted data with enhanced change calculation - FIXED VERSION."""
        try:
            parsed = {}
            
            # Parse price (required)
            if 'price' in data:
                price_value = data['price']
                if isinstance(price_value, (Decimal, float, int)):
                    price = Decimal(str(price_value))
                else:
                    price = parse_financial_value(str(price_value))
                
                if price and price > 0:
                    parsed['price'] = price
                else:
                    self.logger.error(f"Invalid price for {symbol}: {data['price']}")
                    return None
            else:
                return None
            
            # Parse other basic fields
            for field, parsed_key in [
                ('volume', 'volume'),
                ('open', 'open'),
                ('previous_close', 'previous_close'),
                ('high', 'high'),
                ('low', 'low'),
            ]:
                if field in data and data[field] is not None:
                    if field == 'volume':
                        if isinstance(data[field], int):
                            parsed[parsed_key] = data[field]
                        elif isinstance(data[field], (float, Decimal)):
                            parsed[parsed_key] = int(data[field])
                        else:
                            volume = parse_volume(str(data[field]))
                            if volume is not None and volume >= 0:
                                parsed[parsed_key] = volume
                    else:
                        if isinstance(data[field], (Decimal, float, int)):
                            value = Decimal(str(data[field]))
                        else:
                            value = parse_financial_value(str(data[field]))
                        
                        if value is not None and value > 0:
                            parsed[parsed_key] = value
            
            # ENHANCED: Parse changes with multiple strategies
            nominal_change, percentage_change = self._extract_and_calculate_changes(data, parsed, symbol)
            parsed['daily_change_nominal'] = nominal_change
            parsed['daily_change_percent'] = percentage_change
            
            # Set defaults for missing fields
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
            
            # Validation: ensure high >= price >= low
            if parsed['price'] > parsed['high']:
                parsed['high'] = parsed['price']
            if parsed['price'] < parsed['low']:
                parsed['low'] = parsed['price']
            
            self.logger.debug(f"Successfully parsed data for {symbol}: Price=${parsed['price']}, Change=${parsed['daily_change_nominal']} ({parsed['daily_change_percent']}%)")
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error in enhanced parsing for {symbol}: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _extract_and_calculate_changes(self, data: Dict[str, Any], parsed: Dict[str, Any], symbol: str) -> tuple[Decimal, Decimal]:
        """Extract or calculate daily changes with multiple fallback strategies."""
        
        # Strategy 1: Use directly extracted change data from testid selectors
        if self._has_direct_change_data(data):
            return self._use_direct_change_data(data, parsed, symbol)
        
        # Strategy 2: Calculate manually from current price and previous close
        if self._can_calculate_from_prices(parsed):
            return self._calculate_from_prices(parsed, symbol)
        
        # Strategy 3: Try to parse combined change display (e.g., "+2.15 (+1.45%)")
        if self._can_extract_from_combined_display(data):
            return self._extract_from_combined_display(data, symbol)
        
        # Strategy 4: Zero fallback
        self.logger.warning(f"No change data available for {symbol}, using zero values")
        return Decimal('0'), Decimal('0')
    
    def _has_direct_change_data(self, data: Dict[str, Any]) -> bool:
        """Check if we have direct change data from testid selectors."""
        has_nominal = 'change' in data and data['change'] is not None
        has_percent = 'change_percent' in data and data['change_percent'] is not None
        return has_nominal or has_percent
    
    def _use_direct_change_data(self, data: Dict[str, Any], parsed: Dict[str, Any], symbol: str) -> tuple[Decimal, Decimal]:
        """Use directly extracted change data from testid selectors."""
        nominal_change = Decimal('0')
        percentage_change = Decimal('0')
        
        # Extract nominal change
        if 'change' in data and data['change'] is not None:
            if isinstance(data['change'], (Decimal, float, int)):
                nominal_change = Decimal(str(data['change']))
            else:
                nominal_change = parse_financial_value(str(data['change'])) or Decimal('0')
        
        # Extract percentage change  
        if 'change_percent' in data and data['change_percent'] is not None:
            if isinstance(data['change_percent'], (Decimal, float, int)):
                percentage_change = Decimal(str(data['change_percent']))
            else:
                percentage_change = parse_financial_value(str(data['change_percent'])) or Decimal('0')
        
        # If we only have one, calculate the other
        if nominal_change != Decimal('0') and percentage_change == Decimal('0'):
            # Calculate percentage from nominal and previous close
            if 'previous_close' in parsed and parsed['previous_close'] != Decimal('0'):
                percentage_change = (nominal_change / parsed['previous_close']) * 100
        
        elif percentage_change != Decimal('0') and nominal_change == Decimal('0'):
            # Calculate nominal from percentage and previous close
            if 'previous_close' in parsed:
                nominal_change = (percentage_change * parsed['previous_close']) / 100
        
        self.logger.debug(f"Direct change data for {symbol}: ${nominal_change}, {percentage_change}%")
        return nominal_change, percentage_change
    
    def _can_calculate_from_prices(self, parsed: Dict[str, Any]) -> bool:
        """Check if we can calculate changes from current and previous prices."""
        has_current = 'price' in parsed and parsed['price'] is not None
        has_previous = 'previous_close' in parsed and parsed['previous_close'] is not None
        return has_current and has_previous and parsed['previous_close'] != Decimal('0')
    
    def _calculate_from_prices(self, parsed: Dict[str, Any], symbol: str) -> tuple[Decimal, Decimal]:
        """Calculate changes manually from current price and previous close."""
        try:
            current_price = parsed['price']
            previous_close = parsed['previous_close']
            
            # Calculate nominal change
            nominal_change = current_price - previous_close
            
            # Calculate percentage change
            percentage_change = (nominal_change / previous_close) * 100
            
            self.logger.info(f"✅ Calculated changes for {symbol}: ${nominal_change:.2f} ({percentage_change:.2f}%)")
            return nominal_change, percentage_change
            
        except Exception as e:
            self.logger.error(f"Error calculating changes from prices for {symbol}: {e}")
            return Decimal('0'), Decimal('0')
    
    def _can_extract_from_combined_display(self, data: Dict[str, Any]) -> bool:
        """Check if we can extract from combined change display."""
        # Look for fields that might contain combined change info
        combined_fields = ['change_combined', 'price_change_display', 'change_display']
        return any(field in data and data[field] for field in combined_fields)
    
    def _extract_from_combined_display(self, data: Dict[str, Any], symbol: str) -> tuple[Decimal, Decimal]:
        """Extract changes from combined display string like '+2.15 (+1.45%)'."""
        import re
        
        combined_fields = ['change_combined', 'price_change_display', 'change_display']
        
        for field in combined_fields:
            if field in data and data[field]:
                text = str(data[field]).strip()
                
                # Pattern to match: "+2.15 (+1.45%)" or "-0.50 (-0.33%)"
                pattern = r'([+-]?\d+\.?\d*)\s*\(([+-]?\d+\.?\d*)%\)'
                match = re.search(pattern, text)
                
                if match:
                    try:
                        nominal_str = match.group(1)
                        percent_str = match.group(2)
                        
                        nominal_change = Decimal(nominal_str)
                        percentage_change = Decimal(percent_str)
                        
                        self.logger.debug(f"Extracted from combined display for {symbol}: {text} -> ${nominal_change}, {percentage_change}%")
                        return nominal_change, percentage_change
                        
                    except Exception as e:
                        self.logger.debug(f"Error parsing combined display '{text}' for {symbol}: {e}")
                        continue
        
        return Decimal('0'), Decimal('0')
    
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
    
    def scrape_batch(self, symbols: List[str]) -> BatchResult:
        """Scrape multiple symbols and return batch results."""
        start_time = datetime.now(UTC)
        results = []
        successful = 0
        failed = 0
        
        self.logger.info(f"Starting batch scrape of {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols):
            try:
                self.logger.info(f"[{i+1}/{len(symbols)}] Scraping {symbol}...")
                
                stock_data = self.scrape_symbol(symbol)
                if stock_data:
                    result = ScrapingResult(symbol=symbol, success=True, data=stock_data)
                    successful += 1
                else:
                    result = ScrapingResult(symbol=symbol, success=False, error="No data returned")
                    failed += 1
                    self.logger.warning(f"❌ {symbol}: No data returned")
                
                results.append(result)
                
            except Exception as e:
                result = ScrapingResult(symbol=symbol, success=False, error=str(e))
                results.append(result)
                failed += 1
                self.logger.warning(f"❌ {symbol}: {e}")
        
        end_time = datetime.now(UTC)
        duration = (end_time - start_time).total_seconds()
        
        batch_result = BatchResult(
            total_symbols=len(symbols),
            successful=successful,
            failed=failed,
            results=results,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration
        )
        
        # Log market state statistics
        self.logger.info(
            f"Batch scrape completed: {successful}/{len(symbols)} successful "
            f"({batch_result.success_rate:.1f}%) in {duration:.1f}s"
        )
        self.logger.info(
            f"Market state distribution - Regular: {self.stats['market_hours_data']}, "
            f"Pre-market: {self.stats['pre_market_data']}, "
            f"After-hours: {self.stats['after_hours_data']}"
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
            'timestamp': datetime.now(UTC).isoformat()
        }
    
    def reset_stats(self):
        """Reset scraper statistics."""
        self.stats = {
            'requests_made': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'rate_limit_hits': 0,
            'timeout_errors': 0,
            'parsing_errors': 0,
            'market_hours_data': 0,
            'after_hours_data': 0,
            'pre_market_data': 0
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