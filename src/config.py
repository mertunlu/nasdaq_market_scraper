"""Configuration management for NASDAQ-100 scraper."""

import os
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration settings for the NASDAQ-100 scraper."""
    
    # Environment settings
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')
    
    # AWS Configuration
    AWS_REGION = os.getenv('AWS_REGION', 'eu-central-1')
    DYNAMODB_TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'nasdaq_stocks')
    
    # Scraping Configuration
    YAHOO_FINANCE_BASE_URL = "https://finance.yahoo.com/quote/"
    SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '300' if DEBUG else '60'))  # seconds
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '10'))  # seconds
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = float(os.getenv('RETRY_DELAY', '2.0'))  # seconds
    
    # Rate limiting
    RATE_LIMIT_REQUESTS = int(os.getenv('RATE_LIMIT_REQUESTS', '30'))  # per minute
    RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))  # seconds
    REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '2.0'))  # seconds between requests
    
    # Batch processing
    MAX_SYMBOLS_PER_BATCH = int(os.getenv('MAX_SYMBOLS_PER_BATCH', '5' if DEBUG else '100'))
    
    # Health check settings
    HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # seconds
    GRACEFUL_SHUTDOWN_TIMEOUT = int(os.getenv('GRACEFUL_SHUTDOWN_TIMEOUT', '30'))  # seconds
    
    # Data validation
    MIN_PRICE = float(os.getenv('MIN_PRICE', '0.01'))  # Minimum valid price
    MAX_PRICE = float(os.getenv('MAX_PRICE', '10000.0'))  # Maximum reasonable price
    MIN_VOLUME = int(os.getenv('MIN_VOLUME', '0'))  # Minimum valid volume
    
    # File paths
    NASDAQ_SYMBOLS_FILE = os.getenv('NASDAQ_SYMBOLS_FILE', 'data/nasdaq100_symbols.json')
    LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'logs/scraper.log')
    
    # User agent rotation
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
    ]
    
    # Request headers
    DEFAULT_HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    @classmethod
    def get_production_config(cls) -> Dict[str, Any]:
        """Get production-optimized configuration."""
        return {
            'LOG_LEVEL': 'INFO',
            'SCRAPE_INTERVAL': 60,
            'MAX_RETRIES': 3,
            'REQUEST_TIMEOUT': 10,
            'RATE_LIMIT_REQUESTS': 30,
            'RATE_LIMIT_WINDOW': 60,
            'HEALTH_CHECK_INTERVAL': 300,
            'GRACEFUL_SHUTDOWN_TIMEOUT': 30,
            'DEBUG': False
        }
    
    @classmethod
    def get_development_config(cls) -> Dict[str, Any]:
        """Get development configuration."""
        return {
            'LOG_LEVEL': 'DEBUG',
            'SCRAPE_INTERVAL': 300,  # 5 minutes for development
            'MAX_RETRIES': 2,
            'REQUEST_TIMEOUT': 15,
            'RATE_LIMIT_REQUESTS': 10,
            'RATE_LIMIT_WINDOW': 60,
            'MAX_SYMBOLS_PER_BATCH': 5,
            'DEBUG': True
        }
    
    @classmethod
    def validate_config(cls) -> List[str]:
        """Validate configuration settings and return list of issues."""
        import os  # Add this line
        import os.path  # Add this line too
        issues = []
        
        # Check required environment variables
        if not os.getenv('AWS_REGION'):
            issues.append("AWS_REGION not set")
            
        # Validate numeric ranges
        if cls.SCRAPE_INTERVAL < 30:
            issues.append("SCRAPE_INTERVAL too low (minimum 30 seconds)")
            
        if cls.REQUEST_TIMEOUT < 5:
            issues.append("REQUEST_TIMEOUT too low (minimum 5 seconds)")
            
        if cls.MAX_RETRIES < 1 or cls.MAX_RETRIES > 10:
            issues.append("MAX_RETRIES should be between 1 and 10")
            
        if cls.RATE_LIMIT_REQUESTS < 1:
            issues.append("RATE_LIMIT_REQUESTS must be positive")
            
        # Check file paths
        import os.path
        symbols_dir = os.path.dirname(cls.NASDAQ_SYMBOLS_FILE)
        if not os.path.exists(symbols_dir):
            issues.append(f"Symbols file directory does not exist: {symbols_dir}")
            
        logs_dir = os.path.dirname(cls.LOG_FILE_PATH)
        if not os.path.exists(logs_dir):
            issues.append(f"Logs directory does not exist: {logs_dir}")
            
        return issues


# Global configuration instance
config = Config()

# Validate configuration on import
config_issues = config.validate_config()
if config_issues:
    print("Configuration issues found:")
    for issue in config_issues:
        print(f"  - {issue}")
    if not config.DEBUG:
        raise RuntimeError("Configuration validation failed in production mode")