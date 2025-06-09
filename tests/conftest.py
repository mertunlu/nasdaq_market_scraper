"""Pytest configuration and shared fixtures for NASDAQ-100 scraper tests."""

import os
import sys
import pytest
import json
import tempfile
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.models import StockData, ScrapingResult, BatchResult
from src.config import Config
from tests import TEST_CONFIG


@pytest.fixture(scope="session")
def test_config():
    """Test configuration settings."""
    return TEST_CONFIG


@pytest.fixture
def sample_stock_data():
    """Sample StockData object for testing."""
    return StockData(
        symbol="AAPL",
        price=Decimal("150.25"),
        daily_change_percent=Decimal("1.45"),
        daily_change_nominal=Decimal("2.15"),
        volume=45123456,
        high=Decimal("152.10"),
        low=Decimal("148.50"),
        last_updated="2024-01-01T12:00:00Z",
        market="NASDAQ"
    )


@pytest.fixture
def sample_stock_data_list():
    """List of sample StockData objects for batch testing."""
    stocks = []
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    base_price = 100.0
    
    for i, symbol in enumerate(symbols):
        price = Decimal(str(base_price + i * 50))
        stocks.append(StockData(
            symbol=symbol,
            price=price,
            daily_change_percent=Decimal(str(1.0 + i * 0.5)),
            daily_change_nominal=Decimal(str(price * Decimal("0.01"))),
            volume=1000000 + i * 500000,
            high=price + Decimal("5.0"),
            low=price - Decimal("3.0"),
            last_updated=datetime.utcnow().isoformat() + 'Z',
            market="NASDAQ"
        ))
    
    return stocks


@pytest.fixture
def mock_yahoo_response():
    """Mock Yahoo Finance HTML response."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>AAPL Stock Quote</title></head>
    <body>
        <fin-streamer data-field="regularMarketPrice" value="150.25">150.25</fin-streamer>
        <fin-streamer data-field="regularMarketChange" value="2.15">+2.15</fin-streamer>
        <fin-streamer data-field="regularMarketChangePercent" value="1.45">+1.45%</fin-streamer>
        <fin-streamer data-field="regularMarketVolume" value="45123456">45,123,456</fin-streamer>
        <fin-streamer data-field="regularMarketDayHigh" value="152.10">152.10</fin-streamer>
        <fin-streamer data-field="regularMarketDayLow" value="148.50">148.50</fin-streamer>
        <td data-test="DAYS_RANGE-value">148.50 - 152.10</td>
    </body>
    </html>
    """


@pytest.fixture
def mock_yahoo_response_alternative():
    """Alternative mock Yahoo Finance HTML response with different selectors."""
    return """
    <!DOCTYPE html>
    <html>
    <body>
        <div data-field="regularMarketPrice">150.25</div>
        <div data-field="regularMarketChange">2.15</div>
        <div data-field="regularMarketChangePercent">1.45%</div>
        <div data-field="regularMarketVolume">45,123,456</div>
        <div data-field="regularMarketDayHigh">152.10</div>
        <div data-field="regularMarketDayLow">148.50</div>
    </body>
    </html>
    """


@pytest.fixture
def mock_requests_session():
    """Mock requests session for testing."""
    session = Mock()
    response = Mock()
    response.status_code = 200
    response.content = b"mock content"
    response.text = "mock text"
    response.raise_for_status = Mock()
    response.elapsed.total_seconds.return_value = 0.5
    
    session.get.return_value = response
    return session


@pytest.fixture
def mock_dynamodb_table():
    """Mock DynamoDB table for testing."""
    table = Mock()
    
    # Mock successful responses
    table.put_item.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200}
    }
    
    table.get_item.return_value = {
        'Item': {
            'symbol': 'AAPL',
            'price': Decimal('150.25'),
            'daily_change_percent': Decimal('1.45'),
            'daily_change_nominal': Decimal('2.15'),
            'volume': 45123456,
            'high': Decimal('152.10'),
            'low': Decimal('148.50'),
            'last_updated': '2024-01-01T12:00:00Z',
            'market': 'NASDAQ'
        }
    }
    
    table.scan.return_value = {
        'Items': [
            {
                'symbol': 'AAPL',
                'price': Decimal('150.25'),
                'daily_change_percent': Decimal('1.45'),
                'daily_change_nominal': Decimal('2.15'),
                'volume': 45123456,
                'high': Decimal('152.10'),
                'low': Decimal('148.50'),
                'last_updated': '2024-01-01T12:00:00Z',
                'market': 'NASDAQ'
            }
        ]
    }
    
    table.delete_item.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200}
    }
    
    # Mock table metadata
    table.meta.client.describe_table.return_value = {
        'Table': {
            'TableName': 'test_nasdaq_stocks',
            'TableStatus': 'ACTIVE',
            'TableSizeBytes': 1024,
            'CreationDateTime': datetime.utcnow(),
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'}
        }
    }
    
    return table


@pytest.fixture
def mock_boto3_resource():
    """Mock boto3 DynamoDB resource."""
    with patch('boto3.resource') as mock_resource:
        dynamodb = Mock()
        table = mock_dynamodb_table()
        dynamodb.Table.return_value = table
        mock_resource.return_value = dynamodb
        yield mock_resource


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 DynamoDB client."""
    with patch('boto3.client') as mock_client:
        client = Mock()
        
        client.describe_table.return_value = {
            'Table': {
                'TableName': 'test_nasdaq_stocks',
                'TableStatus': 'ACTIVE',
                'TableSizeBytes': 1024,
                'CreationDateTime': datetime.utcnow(),
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'}
            }
        }
        
        client.batch_get_item.return_value = {
            'Responses': {
                'test_nasdaq_stocks': [
                    {
                        'symbol': 'AAPL',
                        'price': Decimal('150.25'),
                        'daily_change_percent': Decimal('1.45'),
                        'daily_change_nominal': Decimal('2.15'),
                        'volume': 45123456,
                        'high': Decimal('152.10'),
                        'low': Decimal('148.50'),
                        'last_updated': '2024-01-01T12:00:00Z',
                        'market': 'NASDAQ'
                    }
                ]
            }
        }
        
        mock_client.return_value = client
        yield mock_client


@pytest.fixture
def temp_symbols_file():
    """Create temporary symbols file for testing."""
    symbols_data = {
        "description": "Test NASDAQ symbols",
        "last_updated": "2024-01-01T12:00:00Z",
        "count": 5,
        "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(symbols_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    os.unlink(temp_file)


@pytest.fixture
def temp_log_file():
    """Create temporary log file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    config = Mock()
    config.DEBUG = True
    config.LOG_LEVEL = 'DEBUG'
    config.AWS_REGION = 'us-east-1'
    config.DYNAMODB_TABLE_NAME = 'test_nasdaq_stocks'
    config.YAHOO_FINANCE_BASE_URL = 'https://finance.yahoo.com/quote/'
    config.SCRAPE_INTERVAL = 300
    config.REQUEST_TIMEOUT = 10
    config.MAX_RETRIES = 3
    config.RETRY_DELAY = 2.0
    config.RATE_LIMIT_REQUESTS = 5
    config.RATE_LIMIT_WINDOW = 60
    config.REQUEST_DELAY = 1.0
    config.MAX_SYMBOLS_PER_BATCH = 5
    config.MIN_PRICE = 0.01
    config.MAX_PRICE = 10000.0
    config.MIN_VOLUME = 0
    config.NASDAQ_SYMBOLS_FILE = 'test_symbols.json'
    config.LOG_FILE_PATH = 'test.log'
    config.USER_AGENTS = ['Mozilla/5.0 (Test Agent)']
    config.DEFAULT_HEADERS = {'User-Agent': 'Test'}
    return config


@pytest.fixture
def scraping_result_success():
    """Sample successful scraping result."""
    stock_data = StockData(
        symbol="AAPL",
        price=Decimal("150.25"),
        daily_change_percent=Decimal("1.45"),
        daily_change_nominal=Decimal("2.15"),
        volume=45123456,
        high=Decimal("152.10"),
        low=Decimal("148.50"),
        last_updated="2024-01-01T12:00:00Z",
        market="NASDAQ"
    )
    
    return ScrapingResult(
        symbol="AAPL",
        success=True,
        data=stock_data,
        timestamp="2024-01-01T12:00:00Z"
    )


@pytest.fixture
def scraping_result_failure():
    """Sample failed scraping result."""
    return ScrapingResult(
        symbol="INVALID",
        success=False,
        error="Symbol not found",
        timestamp="2024-01-01T12:00:00Z"
    )


@pytest.fixture
def batch_result_sample():
    """Sample batch result for testing."""
    results = [
        ScrapingResult(symbol="AAPL", success=True, data=None),
        ScrapingResult(symbol="MSFT", success=True, data=None),
        ScrapingResult(symbol="INVALID", success=False, error="Not found")
    ]
    
    return BatchResult(
        total_symbols=3,
        successful=2,
        failed=1,
        results=results,
        start_time="2024-01-01T12:00:00Z",
        end_time="2024-01-01T12:01:00Z",
        duration_seconds=60.0
    )


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables before each test."""
    # Store original environment
    original_env = os.environ.copy()
    
    # Set test environment variables
    test_env = {
        'DEBUG': 'true',
        'LOG_LEVEL': 'DEBUG',
        'AWS_REGION': 'us-east-1',
        'DYNAMODB_TABLE_NAME': 'test_nasdaq_stocks',
        'SCRAPE_INTERVAL': '300',
        'MAX_SYMBOLS_PER_BATCH': '5'
    }
    
    for key, value in test_env.items():
        os.environ[key] = value
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_health_responses():
    """Mock responses for health check testing."""
    return {
        'database_healthy': True,
        'internet_healthy': True,
        'yahoo_finance_response': {
            'status_code': 200,
            'content': b'test content',
            'elapsed': Mock(total_seconds=Mock(return_value=0.5))
        }
    }


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "aws: mark test as requiring AWS access"
    )
    config.addinivalue_line(
        "markers", "network: mark test as requiring network access"
    )


# Custom pytest hooks
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file names."""
    for item in items:
        # Add markers based on test file names
        if "test_integration" in item.fspath.basename:
            item.add_marker(pytest.mark.integration)
        elif "test_" in item.fspath.basename:
            item.add_marker(pytest.mark.unit)
        
        # Add slow marker for tests that might be slow
        if any(keyword in item.name.lower() for keyword in ['batch', 'full', 'comprehensive']):
            item.add_marker(pytest.mark.slow)