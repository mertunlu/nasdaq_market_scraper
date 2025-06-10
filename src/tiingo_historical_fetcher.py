"""Tiingo API integration for historical NASDAQ-100 data fetching."""

import requests
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import time

from src.config import config
from src.utils import retry_with_backoff, RateLimiter
from src.exceptions import NetworkError, DataValidationError, ConfigurationError
from src.models import StockData


@dataclass
class HistoricalStockData:
    """Historical stock data model for single day."""
    
    symbol: str
    date: str  # YYYY-MM-DD format
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    daily_change_nominal: Decimal  # close - previous_close
    daily_change_percent: Decimal  # (close - previous_close) / previous_close * 100
    previous_close: Decimal
    market: str = "NASDAQ"
    
    def __post_init__(self):
        """Validate data after initialization."""
        self.symbol = self.symbol.upper()
        
        # Convert to Decimal for financial precision
        for field in ['open', 'high', 'low', 'close', 'daily_change_nominal', 
                     'daily_change_percent', 'previous_close']:
            value = getattr(self, field)
            if not isinstance(value, Decimal):
                setattr(self, field, Decimal(str(value)))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'symbol': self.symbol,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'daily_change_nominal': self.daily_change_nominal,
            'daily_change_percent': self.daily_change_percent,
            'previous_close': self.previous_close,
            'market': self.market
        }


class TiingoHistoricalFetcher:
    """Fetches historical stock data from Tiingo API."""
    
    def __init__(self, api_token: str = None):
        self.logger = logging.getLogger(__name__)
        self.api_token = api_token or self._get_api_token()
        self.base_url = "https://api.tiingo.com/tiingo/daily"
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(max_requests=50, time_window=60)  # Tiingo allows more requests
        
        # Set up session headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': f'Token {self.api_token}'
        })
        
        self.logger.info("Tiingo historical fetcher initialized")
    
    def _get_api_token(self) -> str:
        """Get Tiingo API token from environment or config."""
        import os
        token = os.getenv('TIINGO_API_TOKEN')
        if not token:
            raise ConfigurationError(
                "Tiingo API token not found. Set TIINGO_API_TOKEN environment variable."
            )
        return token
    
    @retry_with_backoff(max_retries=3)
    def fetch_historical_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str = None
    ) -> List[HistoricalStockData]:
        """Fetch historical data for a single symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (default: today)
            
        Returns:
            List of HistoricalStockData objects sorted by date
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Rate limiting
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.base_url}/{symbol.upper()}/prices"
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'format': 'json'
        }
        
        try:
            self.logger.debug(f"Fetching historical data for {symbol} from {start_date} to {end_date}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                self.logger.warning(f"No historical data returned for {symbol}")
                return []
            
            historical_data = self._process_historical_data(symbol, data)
            
            self.logger.info(f"Fetched {len(historical_data)} historical records for {symbol}")
            return historical_data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Symbol {symbol} not found in Tiingo")
                return []
            elif e.response.status_code == 429:
                self.logger.warning(f"Rate limited by Tiingo API for {symbol}")
                time.sleep(60)  # Wait a minute and let retry mechanism handle it
                raise NetworkError(f"Rate limited for {symbol}", symbol=symbol)
            else:
                raise NetworkError(f"HTTP error {e.response.status_code} for {symbol}: {e}", symbol=symbol)
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Network error fetching {symbol}: {e}", symbol=symbol)
        except Exception as e:
            raise DataValidationError(f"Error processing {symbol}: {e}", symbol=symbol)
    
    def _process_historical_data(self, symbol: str, raw_data: List[Dict]) -> List[HistoricalStockData]:
        """Process raw API response into HistoricalStockData objects."""
        processed_data = []
        previous_close = None
        
        # Sort by date to ensure chronological order
        raw_data.sort(key=lambda x: x['date'])
        
        for i, day_data in enumerate(raw_data):
            try:
                # Extract basic price data
                date = day_data['date'][:10]  # Take only YYYY-MM-DD part
                open_price = Decimal(str(day_data['open']))
                high_price = Decimal(str(day_data['high']))
                low_price = Decimal(str(day_data['low']))
                close_price = Decimal(str(day_data['close']))
                volume = int(day_data['volume']) if day_data['volume'] else 0
                
                # Calculate daily changes
                if previous_close is not None:
                    daily_change_nominal = close_price - previous_close
                    daily_change_percent = (daily_change_nominal / previous_close) * 100
                else:
                    # For the first day, we don't have previous close
                    daily_change_nominal = Decimal('0')
                    daily_change_percent = Decimal('0')
                    # Use the opening price as previous close for first day
                    previous_close = open_price
                
                # Create historical stock data object
                historical_stock = HistoricalStockData(
                    symbol=symbol,
                    date=date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    daily_change_nominal=daily_change_nominal,
                    daily_change_percent=daily_change_percent,
                    previous_close=previous_close
                )
                
                # Validate the data
                if self._validate_historical_data(historical_stock):
                    processed_data.append(historical_stock)
                else:
                    self.logger.warning(f"Invalid data for {symbol} on {date}, skipping")
                
                # Update previous close for next iteration
                previous_close = close_price
                
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error processing data for {symbol} on {date}: {e}")
                continue
        
        return processed_data
    
    def _validate_historical_data(self, data: HistoricalStockData) -> bool:
        """Validate historical stock data."""
        try:
            # Basic validations
            if data.high < data.low:
                return False
            
            if data.close < 0 or data.open < 0:
                return False
            
            if data.volume < 0:
                return False
            
            # Check if close is within high/low range
            if not (data.low <= data.close <= data.high):
                return False
            
            # Check if open is within high/low range
            if not (data.low <= data.open <= data.high):
                return False
            
            return True
            
        except Exception:
            return False
    
    def fetch_batch_historical_data(
        self, 
        symbols: List[str], 
        start_date: str, 
        end_date: str = None,
        delay_between_requests: float = 1.0
    ) -> Dict[str, List[HistoricalStockData]]:
        """Fetch historical data for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (default: today)
            delay_between_requests: Delay between API requests in seconds
            
        Returns:
            Dictionary mapping symbols to their historical data
        """
        results = {}
        failed_symbols = []
        
        self.logger.info(f"Fetching historical data for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols):
            try:
                historical_data = self.fetch_historical_data(symbol, start_date, end_date)
                results[symbol] = historical_data
                
                if historical_data:
                    self.logger.debug(f"✓ {symbol}: {len(historical_data)} records")
                else:
                    self.logger.warning(f"✗ {symbol}: No data returned")
                    failed_symbols.append(symbol)
                
            except Exception as e:
                self.logger.error(f"✗ {symbol}: {e}")
                failed_symbols.append(symbol)
                results[symbol] = []
            
            # Add delay between requests to be respectful
            if i < len(symbols) - 1:  # Don't sleep after the last request
                time.sleep(delay_between_requests)
        
        success_count = len([s for s in results if results[s]])
        self.logger.info(
            f"Batch fetch completed: {success_count}/{len(symbols)} successful, "
            f"{len(failed_symbols)} failed"
        )
        
        if failed_symbols:
            self.logger.warning(f"Failed symbols: {failed_symbols}")
        
        return results
    
    def get_one_year_data(self, symbols: List[str]) -> Dict[str, List[HistoricalStockData]]:
        """Fetch 1 year of historical data for given symbols.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to their historical data
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"Fetching 1 year of data from {start_date_str} to {end_date_str}")
        
        return self.fetch_batch_historical_data(symbols, start_date_str, end_date_str)
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get metadata information for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with symbol metadata or None if not found
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{self.base_url}/{symbol.upper()}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Symbol {symbol} not found")
                return None
            else:
                raise NetworkError(f"Error getting info for {symbol}: {e}")
        except Exception as e:
            self.logger.error(f"Error getting symbol info for {symbol}: {e}")
            return None
    
    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
            self.logger.debug("Tiingo session closed")


class HistoricalDataManager:
    """Manages historical data fetching and storage operations."""
    
    def __init__(self, tiingo_fetcher: TiingoHistoricalFetcher = None, db_manager = None):
        self.logger = logging.getLogger(__name__)
        self.tiingo_fetcher = tiingo_fetcher or TiingoHistoricalFetcher()
        self.db_manager = db_manager  # Will be injected from main app
        
    def fetch_and_store_one_year_data(self, symbols: List[str] = None) -> Tuple[int, int]:
        """Fetch and store 1 year of historical data for NASDAQ-100 symbols.
        
        Args:
            symbols: List of symbols to fetch (default: load from config)
            
        Returns:
            Tuple of (successful_symbols, total_records_saved)
        """
        if symbols is None:
            from src.utils import load_nasdaq_symbols
            symbols = load_nasdaq_symbols()
        
        self.logger.info(f"Starting historical data fetch for {len(symbols)} symbols")
        
        # Fetch historical data
        historical_data = self.tiingo_fetcher.get_one_year_data(symbols)
        
        # Store in database
        total_records = 0
        successful_symbols = 0
        
        for symbol, data_list in historical_data.items():
            if data_list:
                try:
                    # Save to database (assuming we have a historical database manager)
                    if self.db_manager and hasattr(self.db_manager, 'save_historical_data'):
                        saved_count = self.db_manager.save_historical_data(symbol, data_list)
                        total_records += saved_count
                        successful_symbols += 1
                        self.logger.info(f"Saved {saved_count} records for {symbol}")
                    else:
                        self.logger.warning("No database manager configured for historical data")
                except Exception as e:
                    self.logger.error(f"Failed to save historical data for {symbol}: {e}")
        
        self.logger.info(
            f"Historical data fetch completed: {successful_symbols} symbols, "
            f"{total_records} total records saved"
        )
        
        return successful_symbols, total_records
    
    def update_missing_data(self, symbols: List[str] = None, days_back: int = 30):
        """Update missing or recent historical data.
        
        Args:
            symbols: List of symbols to update
            days_back: Number of days back to fetch
        """
        if symbols is None:
            from src.utils import load_nasdaq_symbols
            symbols = load_nasdaq_symbols()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"Updating historical data for last {days_back} days")
        
        historical_data = self.tiingo_fetcher.fetch_batch_historical_data(
            symbols, start_date_str, end_date_str
        )
        
        # Store updates in database
        for symbol, data_list in historical_data.items():
            if data_list and self.db_manager:
                try:
                    if hasattr(self.db_manager, 'update_historical_data'):
                        self.db_manager.update_historical_data(symbol, data_list)
                except Exception as e:
                    self.logger.error(f"Failed to update historical data for {symbol}: {e}")


# Example usage and testing
if __name__ == "__main__":
    import os
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # You need to set your Tiingo API token
    # os.environ['TIINGO_API_TOKEN'] = 'your-api-token-here'
    
    try:
        # Initialize fetcher
        fetcher = TiingoHistoricalFetcher()
        
        # Test with a few symbols
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        
        # Fetch 1 year of data
        historical_data = fetcher.get_one_year_data(test_symbols)
        
        # Print results
        for symbol, data_list in historical_data.items():
            if data_list:
                print(f"\n{symbol}: {len(data_list)} records")
                print(f"  First: {data_list[0].date} - Close: ${data_list[0].close}")
                print(f"  Last:  {data_list[-1].date} - Close: ${data_list[-1].close}")
                print(f"  Last Change: {data_list[-1].daily_change_percent:.2f}%")
            else:
                print(f"\n{symbol}: No data")
        
        fetcher.close()
        
    except Exception as e:
        print(f"Error: {e}")