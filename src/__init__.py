"""NASDAQ-100 Financial Data Scraper Package.

A comprehensive web scraping system for collecting real-time NASDAQ-100 stock data
and storing it in AWS DynamoDB. Designed to integrate with existing financial data
platforms and provide reliable, automated data collection.

Key Components:
- Web scraping from Yahoo Finance with robust error handling
- AWS DynamoDB integration for scalable data storage
- Rate limiting and respectful scraping practices
- Comprehensive health monitoring and alerting
- Docker containerization for easy deployment

Example Usage:
    # Run the scraper daemon
    from src.main import NasdaqScraperApp
    app = NasdaqScraperApp()
    app.run_daemon()
    
    # Or use individual components
    from src.scraper import NasdaqScraper
    from src.database import DynamoDBManager
    
    scraper = NasdaqScraper()
    db = DynamoDBManager()
    
    results = scraper.scrape_all()
    db.save_batch_stock_data(results.get_successful_data())
"""

__version__ = "1.0.0"
__author__ = "NASDAQ Scraper Team"
__email__ = "your-email@domain.com"
__license__ = "MIT"

# Import main classes for easy access
from src.main import NasdaqScraperApp
from src.scraper import NasdaqScraper, YahooFinanceScraper
from src.database import DynamoDBManager
from src.health import HealthChecker
from src.models import StockData, ScrapingResult, BatchResult, HealthStatus
from src.config import config

# Package-level exports
__all__ = [
    'NasdaqScraperApp',
    'NasdaqScraper', 
    'YahooFinanceScraper',
    'DynamoDBManager',
    'HealthChecker',
    'StockData',
    'ScrapingResult', 
    'BatchResult',
    'HealthStatus',
    'config'
]

# Package metadata
PACKAGE_INFO = {
    'name': 'nasdaq-scraper',
    'version': __version__,
    'description': 'NASDAQ-100 stock data scraper with AWS DynamoDB integration',
    'author': __author__,
    'license': __license__,
    'python_requires': '>=3.9',
    'keywords': ['finance', 'stocks', 'nasdaq', 'scraping', 'aws', 'dynamodb'],
    'classifiers': [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ]
}

def get_version():
    """Get package version."""
    return __version__

def get_info():
    """Get package information."""
    return PACKAGE_INFO.copy()