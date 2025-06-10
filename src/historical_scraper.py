"""Historical data scraper for NASDAQ-100 stocks from Yahoo Finance."""

import re
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from src.config import config
from src.exceptions import NetworkError, ParsingError, DataValidationError
from src.utils import (
    get_request_headers, 
    retry_with_backoff, 
    parse_financial_value,
    parse_volume,
    calculate_delay
)


@dataclass
class HistoricalDataPoint:
    """Single day's historical stock data."""
    
    symbol: str
    date: str  # YYYY-MM-DD format
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Decimal
    volume: int
    daily_change_percent: Decimal
    daily_change_nominal: Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DynamoDB storage."""
        return {
            'symbol': self.symbol,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'adj_close': self.adj_close,
            'volume': self.volume,
            'daily_change_percent': self.daily_change_percent,
            'daily_change_nominal': self.daily_change_nominal,
            'created_at': datetime.utcnow().isoformat() + 'Z'
        }


class YahooHistoricalScraper:
    """Scraper for Yahoo Finance historical data."""
    
    def __init__(self, debug: bool = False):
        self.logger = logging.getLogger(__name__)
        self.debug = debug
        self.session = requests.Session()
        self.session.headers.update(get_request_headers())
        
        # Yahoo Finance historical URL pattern
        self.base_url = "https://finance.yahoo.com/quote/{symbol}/history"
        
        # Statistics tracking
        self.stats = {
            'requests_made': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'total_data_points': 0
        }
    
    @retry_with_backoff(max_retries=3)
    def scrape_historical_data(
        self, 
        symbol: str, 
        period: str = "1y",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[HistoricalDataPoint]:
        """
        Scrape historical data for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Time period ('1y', '2y', '5y', 'max') or custom range
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            List of HistoricalDataPoint objects
        """
        self.logger.info(f"Scraping historical data for {symbol} (period: {period})")
        
        try:
            # Build URL with parameters
            url = self._build_history_url(symbol, period, start_date, end_date)
            
            # Make request with retry logic
            response = self._make_request(url, symbol)
            
            # Parse the HTML response
            historical_data = self._parse_historical_data(response.text, symbol)
            
            # Calculate percentage and nominal changes
            historical_data = self._calculate_changes(historical_data)
            
            self.stats['successful_scrapes'] += 1
            self.stats['total_data_points'] += len(historical_data)
            
            self.logger.info(f"Successfully scraped {len(historical_data)} data points for {symbol}")
            return historical_data
            
        except Exception as e:
            self.stats['failed_scrapes'] += 1
            self.logger.error(f"Failed to scrape historical data for {symbol}: {e}")
            raise
    
    def _build_history_url(
        self, 
        symbol: str, 
        period: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> str:
        """Build Yahoo Finance history URL with parameters."""
        
        base_url = f"https://finance.yahoo.com/quote/{symbol}/history"
        
        # If custom date range is provided
        if start_date and end_date:
            # Convert dates to Unix timestamps
            start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
            end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
            return f"{base_url}?period1={start_ts}&period2={end_ts}&interval=1d"
        
        # Use predefined period
        periods = {
            '1y': '1y',
            '2y': '2y', 
            '5y': '5y',
            'max': 'max'
        }
        
        period_param = periods.get(period, '1y')
        return f"{base_url}?range={period_param}&interval=1d"
    
    def _make_request(self, url: str, symbol: str) -> requests.Response:
        """Make HTTP request with proper error handling."""
        self.stats['requests_made'] += 1
        
        try:
            # Add random delay to avoid rate limiting
            time.sleep(config.REQUEST_DELAY)
            
            self.logger.debug(f"Making request to: {url}")
            
            response = self.session.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers=get_request_headers()
            )
            
            # Check for rate limiting
            if response.status_code == 429:
                self.logger.warning(f"Rate limited for {symbol}, waiting...")
                time.sleep(60)  # Wait 1 minute
                response = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            raise NetworkError(f"Request timeout for {symbol}", symbol=symbol)
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed for {symbol}: {e}", symbol=symbol)
    
    def _parse_historical_data(self, html_content: str, symbol: str) -> List[HistoricalDataPoint]:
        """Parse historical data from Yahoo Finance HTML."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the historical data table
            table = soup.find('table', {'data-test': 'historical-prices'})
            if not table:
                # Try alternative selector
                table = soup.find('table')
                if not table:
                    raise ParsingError(f"Could not find historical data table for {symbol}")
            
            tbody = table.find('tbody')
            if not tbody:
                raise ParsingError(f"Could not find table body for {symbol}")
            
            rows = tbody.find_all('tr')
            historical_data = []
            
            for row in rows:
                try:
                    data_point = self._parse_row(row, symbol)
                    if data_point:
                        historical_data.append(data_point)
                except Exception as e:
                    self.logger.warning(f"Failed to parse row for {symbol}: {e}")
                    continue
            
            if not historical_data:
                raise ParsingError(f"No valid historical data found for {symbol}")
            
            # Sort by date (most recent first)
            historical_data.sort(key=lambda x: x.date, reverse=True)
            
            return historical_data
            
        except Exception as e:
            raise ParsingError(f"Failed to parse historical data for {symbol}: {e}")
    
    def _parse_row(self, row, symbol: str) -> Optional[HistoricalDataPoint]:
        """Parse a single row of historical data."""
        cells = row.find_all('td')
        
        if len(cells) < 7:
            return None
        
        try:
            # Extract data from cells
            date_str = cells[0].get_text(strip=True)
            open_str = cells[1].get_text(strip=True)
            high_str = cells[2].get_text(strip=True)
            low_str = cells[3].get_text(strip=True)
            close_str = cells[4].get_text(strip=True)
            adj_close_str = cells[5].get_text(strip=True)
            volume_str = cells[6].get_text(strip=True)
            
            # Skip dividend rows and other non-price data
            if any(text in date_str.lower() for text in ['dividend', 'split']):
                return None
            
            # Parse date
            date = self._parse_date(date_str)
            if not date:
                return None
            
            # Parse numerical values
            open_price = parse_financial_value(open_str)
            high_price = parse_financial_value(high_str)
            low_price = parse_financial_value(low_str)
            close_price = parse_financial_value(close_str)
            adj_close_price = parse_financial_value(adj_close_str)
            volume = parse_volume(volume_str)
            
            # Validate required fields
            if any(val is None for val in [open_price, high_price, low_price, close_price, adj_close_price]):
                self.logger.warning(f"Missing price data for {symbol} on {date}")
                return None
            
            if volume is None:
                volume = 0  # Volume can be 0 for some stocks/dates
            
            # Create data point (changes will be calculated later)
            return HistoricalDataPoint(
                symbol=symbol,
                date=date,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                adj_close=adj_close_price,
                volume=volume,
                daily_change_percent=Decimal('0'),  # Will be calculated
                daily_change_nominal=Decimal('0')   # Will be calculated
            )
            
        except Exception as e:
            self.logger.warning(f"Error parsing row for {symbol}: {e}")
            return None
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format."""
        try:
            # Common date formats from Yahoo Finance
            formats = [
                '%b %d, %Y',    # Jan 15, 2024
                '%B %d, %Y',    # January 15, 2024
                '%m/%d/%Y',     # 01/15/2024
                '%Y-%m-%d'      # 2024-01-15
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If no format matches, return None
            self.logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception:
            return None
    
    def _calculate_changes(self, historical_data: List[HistoricalDataPoint]) -> List[HistoricalDataPoint]:
        """Calculate daily percentage and nominal changes."""
        if len(historical_data) < 2:
            return historical_data
        
        # Sort by date (oldest first) for calculation
        sorted_data = sorted(historical_data, key=lambda x: x.date)
        
        for i in range(len(sorted_data)):
            if i == 0:
                # First day has no previous close
                sorted_data[i].daily_change_percent = Decimal('0')
                sorted_data[i].daily_change_nominal = Decimal('0')
            else:
                # Calculate change from previous day's close
                current_close = sorted_data[i].close
                previous_close = sorted_data[i-1].close
                
                if previous_close != 0:
                    # Nominal change
                    nominal_change = current_close - previous_close
                    
                    # Percentage change
                    percentage_change = (nominal_change / previous_close) * 100
                    
                    sorted_data[i].daily_change_nominal = nominal_change
                    sorted_data[i].daily_change_percent = percentage_change
                else:
                    sorted_data[i].daily_change_percent = Decimal('0')
                    sorted_data[i].daily_change_nominal = Decimal('0')
        
        return sorted_data
    
    def scrape_multiple_symbols(
        self, 
        symbols: List[str], 
        period: str = "1y",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, List[HistoricalDataPoint]]:
        """Scrape historical data for multiple symbols."""
        results = {}
        
        self.logger.info(f"Scraping historical data for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                self.logger.info(f"Processing {symbol} ({i}/{len(symbols)})")
                
                data = self.scrape_historical_data(symbol, period, start_date, end_date)
                results[symbol] = data
                
                # Rate limiting delay between symbols
                if i < len(symbols):  # Don't delay after the last symbol
                    delay = config.REQUEST_DELAY * 2  # Longer delay for batch operations
                    self.logger.debug(f"Waiting {delay}s before next symbol...")
                    time.sleep(delay)
                    
            except Exception as e:
                self.logger.error(f"Failed to scrape {symbol}: {e}")
                results[symbol] = []
        
        success_count = len([v for v in results.values() if v])
        self.logger.info(f"Completed batch scraping: {success_count}/{len(symbols)} successful")
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        total_requests = self.stats['requests_made']
        if total_requests > 0:
            success_rate = (self.stats['successful_scrapes'] / total_requests) * 100
        else:
            success_rate = 0
        
        return {
            'requests_made': total_requests,
            'successful_scrapes': self.stats['successful_scrapes'],
            'failed_scrapes': self.stats['failed_scrapes'],
            'total_data_points': self.stats['total_data_points'],
            'success_rate_percent': success_rate
        }
    
    def close(self):
        """Clean up resources."""
        if self.session:
            self.session.close()


class HistoricalDataValidator:
    """Validates historical stock data."""
    
    @staticmethod
    def validate_data_point(data_point: HistoricalDataPoint) -> bool:
        """Validate a single historical data point."""
        try:
            # Check basic data integrity
            if data_point.high < data_point.low:
                return False
            
            if data_point.close < data_point.low or data_point.close > data_point.high:
                return False
            
            if data_point.open < data_point.low or data_point.open > data_point.high:
                return False
            
            # Check for reasonable price values
            if any(price <= 0 for price in [data_point.open, data_point.high, 
                                          data_point.low, data_point.close, data_point.adj_close]):
                return False
            
            # Check volume is non-negative
            if data_point.volume < 0:
                return False
            
            # Check date format
            try:
                datetime.strptime(data_point.date, '%Y-%m-%d')
            except ValueError:
                return False
            
            return True
            
        except Exception:
            return False
    
    @staticmethod
    def validate_symbol_data(data_points: List[HistoricalDataPoint]) -> Tuple[List[HistoricalDataPoint], List[str]]:
        """Validate all data points for a symbol and return valid ones."""
        valid_points = []
        invalid_reasons = []
        
        for point in data_points:
            if HistoricalDataValidator.validate_data_point(point):
                valid_points.append(point)
            else:
                invalid_reasons.append(f"Invalid data for {point.symbol} on {point.date}")
        
        return valid_points, invalid_reasons