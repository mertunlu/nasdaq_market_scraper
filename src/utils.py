"""Utility functions for the NASDAQ-100 scraper."""

import json
import time
import psutil
import shutil
import random
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Union
from collections import deque
from threading import Lock
from decimal import Decimal

from src.config import config
from src.exceptions import DataValidationError, ConfigurationError
from src.models import StockData


def setup_logging(log_level: str = None, log_file: str = None) -> logging.Logger:
    """Set up structured logging for the application."""
    if log_level is None:
        log_level = config.LOG_LEVEL
    if log_file is None:
        log_file = config.LOG_FILE_PATH
    
    # Create logger
    logger = logging.getLogger('nasdaq_scraper')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO if not config.DEBUG else logging.DEBUG)
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    try:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    except (IOError, OSError) as e:
        logger.warning(f"Could not create file handler for {log_file}: {e}")
    
    return logger


def load_nasdaq_symbols(file_path: str = None) -> List[str]:
    """Load NASDAQ-100 symbols from JSON file."""
    if file_path is None:
        file_path = config.NASDAQ_SYMBOLS_FILE
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Handle different JSON structures
        if isinstance(data, list):
            symbols = [symbol.upper().strip() for symbol in data]
        elif isinstance(data, dict):
            if 'symbols' in data:
                symbols = [symbol.upper().strip() for symbol in data['symbols']]
            elif 'data' in data:
                symbols = [symbol.upper().strip() for symbol in data['data']]
            else:
                # Assume keys are symbols
                symbols = [symbol.upper().strip() for symbol in data.keys()]
        else:
            raise ValueError("Invalid JSON structure in symbols file")
        
        # Validate symbols
        valid_symbols = []
        for symbol in symbols:
            if validate_symbol_format(symbol):
                valid_symbols.append(symbol)
            else:
                logging.warning(f"Invalid symbol format: {symbol}")
        
        if not valid_symbols:
            raise ValueError("No valid symbols found in file")
            
        logging.info(f"Loaded {len(valid_symbols)} valid symbols from {file_path}")
        return valid_symbols
        
    except FileNotFoundError:
        raise ConfigurationError(f"Symbols file not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ConfigurationError(f"Invalid JSON in symbols file: {e}")
    except Exception as e:
        raise ConfigurationError(f"Error loading symbols file: {e}")


def validate_symbol_format(symbol: str) -> bool:
    """Validate stock symbol format."""
    if not symbol or not isinstance(symbol, str):
        return False
    
    # Basic symbol validation (1-10 characters, alphanumeric)
    pattern = r'^[A-Z]{1,10}$'
    return bool(re.match(pattern, symbol.upper()))


def validate_stock_data(data: Dict[str, Any]) -> bool:
    """Validate scraped stock data."""
    required_fields = ['symbol', 'price', 'daily_change_percent', 
                      'daily_change_nominal', 'volume', 'high', 'low', 'open', 'previous_close']
    
    # Check required fields
    for field in required_fields:
        if field not in data or data[field] is None:
            return False
    
    try:
        # Validate data types and ranges
        price = float(data['price'])
        high = float(data['high'])
        low = float(data['low'])
        volume = int(data['volume'])
        
        # Price validation
        if price <= 0 or price < config.MIN_PRICE or price > config.MAX_PRICE:
            return False
        
        # High/Low validation
        if high < low or low < 0:
            return False
        
        # Price within daily range
        if price < low or price > high:
            return False
        
        # Volume validation
        if volume < config.MIN_VOLUME:
            return False
        
        # Symbol validation
        if not validate_symbol_format(data['symbol']):
            return False
        
        return True
        
    except (ValueError, TypeError, KeyError):
        return False


def parse_financial_value(value_str: str) -> Optional[Decimal]:
    """Parse financial value from string, handling various formats."""
    if not value_str or value_str.strip() == '':
        return None
    
    try:
        # Remove common formatting
        cleaned = value_str.strip().replace(',', '').replace('$', '')
        
        # Handle percentage values
        if '%' in cleaned:
            cleaned = cleaned.replace('%', '')
            return Decimal(cleaned)
        
        # Handle negative values in parentheses
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        
        # Handle "N/A" or similar
        if cleaned.upper() in ['N/A', 'NA', 'NULL', 'NONE', '--']:
            return None
        
        return Decimal(cleaned)
        
    except (InvalidOperation, ValueError):
        return None


def parse_volume(volume_str: str) -> Optional[int]:
    """Parse volume from string, handling various formats."""
    if not volume_str or volume_str.strip() == '':
        return None
    
    try:
        cleaned = volume_str.strip().replace(',', '')
        
        # Handle abbreviations (K, M, B)
        multiplier = 1
        if cleaned.upper().endswith('K'):
            multiplier = 1000
            cleaned = cleaned[:-1]
        elif cleaned.upper().endswith('M'):
            multiplier = 1000000
            cleaned = cleaned[:-1]
        elif cleaned.upper().endswith('B'):
            multiplier = 1000000000
            cleaned = cleaned[:-1]
        
        if cleaned.upper() in ['N/A', 'NA', 'NULL', 'NONE', '--']:
            return None
        
        return int(float(cleaned) * multiplier)
        
    except (ValueError, TypeError):
        return None


def get_random_user_agent() -> str:
    """Get random user agent string."""
    return random.choice(config.USER_AGENTS)


def get_request_headers() -> Dict[str, str]:
    """Get request headers with random user agent."""
    headers = config.DEFAULT_HEADERS.copy()
    headers['User-Agent'] = get_random_user_agent()
    return headers


def calculate_delay(attempt: int, base_delay: float = None) -> float:
    """Calculate exponential backoff delay."""
    if base_delay is None:
        base_delay = config.RETRY_DELAY
    
    # Exponential backoff with jitter
    delay = base_delay * (2 ** (attempt - 1))
    jitter = random.uniform(0.1, 0.5)
    return delay + jitter


def get_system_info() -> Dict[str, Any]:
    """Get system information for health checks."""
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'memory_usage_mb': memory.used / (1024 * 1024),
            'memory_percent': memory.percent,
            'disk_space_gb': disk.free / (1024 * 1024 * 1024),
            'disk_usage_percent': (disk.used / disk.total) * 100,
            'cpu_percent': psutil.cpu_percent(interval=1),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    except Exception as e:
        logging.warning(f"Could not get system info: {e}")
        return {
            'memory_usage_mb': 0,
            'disk_space_gb': 0,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }


def ensure_directory_exists(path: str) -> None:
    """Ensure directory exists, create if necessary."""
    import os
    directory = os.path.dirname(path) if os.path.isfile(path) else path
    os.makedirs(directory, exist_ok=True)


class RateLimiter:
    """Thread-safe rate limiter implementation."""
    
    def __init__(self, max_requests: int = None, time_window: int = None):
        self.max_requests = max_requests or config.RATE_LIMIT_REQUESTS
        self.time_window = time_window or config.RATE_LIMIT_WINDOW
        self.requests = deque()
        self.lock = Lock()
    
    def wait_if_needed(self) -> float:
        """Wait if rate limit would be exceeded. Returns wait time."""
        with self.lock:
            now = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()
            
            # Check if we need to wait
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    return sleep_time
            
            # Record this request
            self.requests.append(now)
            return 0.0
    
    def get_current_rate(self) -> float:
        """Get current request rate (requests per minute)."""
        with self.lock:
            now = time.time()
            recent_requests = [req for req in self.requests if req > now - 60]
            return len(recent_requests)


class CircuitBreaker:
    """Simple circuit breaker for handling failures."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half-open
        self.lock = Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        with self.lock:
            if self.state == 'open':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'half-open'
                else:
                    raise Exception("Circuit breaker is open")
            
            try:
                result = func(*args, **kwargs)
                if self.state == 'half-open':
                    self.state = 'closed'
                    self.failure_count = 0
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                
                raise e


def retry_with_backoff(max_retries: int = None, base_delay: float = None):
    """Decorator for retrying functions with exponential backoff."""
    if max_retries is None:
        max_retries = config.MAX_RETRIES
    if base_delay is None:
        base_delay = config.RETRY_DELAY
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    
                    delay = calculate_delay(attempt, base_delay)
                    logging.warning(f"Attempt {attempt} failed for {func.__name__}: {e}. Retrying in {delay:.2f}s")
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator


def clean_string(text: str) -> str:
    """Clean and normalize string data."""
    if not text:
        return ""
    
    # Remove extra whitespace and newlines
    cleaned = re.sub(r'\s+', ' ', text.strip())
    
    # Remove non-printable characters
    cleaned = re.sub(r'[^\x20-\x7E]', '', cleaned)
    
    return cleaned


def format_currency(value: Union[float, Decimal], symbol: str = "$") -> str:
    """Format currency value for display."""
    try:
        if isinstance(value, str):
            value = float(value)
        return f"{symbol}{value:,.2f}"
    except (ValueError, TypeError):
        return "N/A"


def format_percentage(value: Union[float, Decimal]) -> str:
    """Format percentage value for display."""
    try:
        if isinstance(value, str):
            value = float(value)
        return f"{value:+.2f}%"
    except (ValueError, TypeError):
        return "N/A"


def format_volume(volume: int) -> str:
    """Format volume with abbreviations."""
    try:
        if volume >= 1_000_000_000:
            return f"{volume / 1_000_000_000:.2f}B"
        elif volume >= 1_000_000:
            return f"{volume / 1_000_000:.2f}M"
        elif volume >= 1_000:
            return f"{volume / 1_000:.2f}K"
        else:
            return str(volume)
    except (ValueError, TypeError):
        return "N/A"


def chunk_list(items: List, chunk_size: int) -> List[List]:
    """Split list into chunks of specified size."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def get_nasdaq_symbols_sample() -> List[str]:
    """Get a sample of NASDAQ-100 symbols for testing."""
    return [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 
        'ADBE', 'CRM', 'ORCL', 'CSCO', 'INTC', 'AMD', 'QCOM'
    ]


def create_nasdaq_symbols_file(file_path: str = None) -> None:
    """Create a sample NASDAQ-100 symbols file."""
    if file_path is None:
        file_path = config.NASDAQ_SYMBOLS_FILE
    
    # Full NASDAQ-100 symbols (as of 2024)
    nasdaq100_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA',
        'AVGO', 'ASML', 'COST', 'NFLX', 'ADBE', 'PEP', 'TMUS', 'CSCO',
        'ORCL', 'CRM', 'ACN', 'INTC', 'AMD', 'QCOM', 'TXN', 'INTU',
        'CMCSA', 'AMGN', 'HON', 'AMAT', 'PANW', 'VRTX', 'ADI', 'GILD',
        'BKNG', 'MU', 'ADP', 'LRCX', 'SBUX', 'MELI', 'KLAC', 'MDLZ',
        'SNPS', 'CDNS', 'REGN', 'PYPL', 'FTNT', 'MAR', 'MRVL', 'ORLY',
        'CSX', 'DASH', 'ADSK', 'ABNB', 'ROP', 'NXPI', 'WDAY', 'CPRT',
        'MNST', 'FANG', 'AEP', 'ROST', 'KDP', 'PAYX', 'ODFL', 'FAST',
        'BKR', 'EA', 'DDOG', 'VRSK', 'XEL', 'CTSH', 'GEHC', 'KHC',
        'LULU', 'TEAM', 'CSGP', 'IDXX', 'ANSS', 'ZS', 'DXCM', 'CCEP',
        'BIIB', 'TTWO', 'PCAR', 'ON', 'CRWD', 'CDW', 'WBD', 'GFS',
        'ILMN', 'MDB', 'WBA', 'MRNA', 'ARM', 'SMCI'
    ]
    
    data = {
        "description": "NASDAQ-100 Stock Symbols",
        "last_updated": datetime.utcnow().isoformat() + 'Z',
        "count": len(nasdaq100_symbols),
        "symbols": nasdaq100_symbols
    }
    
    ensure_directory_exists(file_path)
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    logging.info(f"Created NASDAQ-100 symbols file with {len(nasdaq100_symbols)} symbols: {file_path}")


def performance_timer(func):
    """Decorator to measure function execution time."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logging.debug(f"{func.__name__} executed in {duration:.3f} seconds")
        return result
    return wrapper


def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float with fallback."""
    try:
        # Handle Decimal objects directly
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            # Clean the string by removing common formatting characters
            cleaned = value.strip().replace(',', '').replace('%', '')
            if cleaned.upper() in ['N/A', 'NA', 'NULL', 'NONE', '--', '']:
                return default
            return float(cleaned)
        else:
            return default
    except (ValueError, TypeError, AttributeError):
        return default

def safe_int_conversion(value: Any, default: int = 0) -> int:
    """Safely convert value to int with fallback."""
    try:
        if isinstance(value, int):
            return value
        elif isinstance(value, float):
            return int(value)
        elif isinstance(value, str):
            cleaned = value.strip().replace(',', '')
            if cleaned.upper() in ['N/A', 'NA', 'NULL', 'NONE', '--', '']:
                return default
            return int(float(cleaned))
        else:
            return default
    except (ValueError, TypeError, AttributeError):
        return default


# Initialize logging when module is imported
logger = setup_logging()