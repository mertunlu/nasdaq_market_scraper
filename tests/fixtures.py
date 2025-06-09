"""Test fixtures and mock data for NASDAQ-100 scraper tests."""

from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any

# Mock Yahoo Finance HTML responses for different scenarios
MOCK_YAHOO_RESPONSE_COMPLETE = """
<!DOCTYPE html>
<html>
<head>
    <title>Apple Inc. (AAPL) Stock Price, News, Quote & History - Yahoo Finance</title>
</head>
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

MOCK_YAHOO_RESPONSE_MINIMAL = """
<!DOCTYPE html>
<html>
<body>
    <fin-streamer data-field="regularMarketPrice" value="150.25">150.25</fin-streamer>
</body>
</html>
"""

MOCK_YAHOO_RESPONSE_ALTERNATIVE_SELECTORS = """
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

MOCK_YAHOO_RESPONSE_NEGATIVE_CHANGE = """
<!DOCTYPE html>
<html>
<body>
    <fin-streamer data-field="regularMarketPrice" value="148.50">148.50</fin-streamer>
    <fin-streamer data-field="regularMarketChange" value="-1.75">-1.75</fin-streamer>
    <fin-streamer data-field="regularMarketChangePercent" value="-1.17">-1.17%</fin-streamer>
    <fin-streamer data-field="regularMarketVolume" value="32456789">32,456,789</fin-streamer>
    <fin-streamer data-field="regularMarketDayHigh" value="150.25">150.25</fin-streamer>
    <fin-streamer data-field="regularMarketDayLow" value="147.80">147.80</fin-streamer>
</body>
</html>
"""

MOCK_YAHOO_RESPONSE_HIGH_VOLUME = """
<!DOCTYPE html>
<html>
<body>
    <fin-streamer data-field="regularMarketPrice" value="275.50">275.50</fin-streamer>
    <fin-streamer data-field="regularMarketChange" value="5.25">+5.25</fin-streamer>
    <fin-streamer data-field="regularMarketChangePercent" value="1.94">+1.94%</fin-streamer>
    <fin-streamer data-field="regularMarketVolume" value="125456789">125.46M</fin-streamer>
    <fin-streamer data-field="regularMarketDayHigh" value="278.10">278.10</fin-streamer>
    <fin-streamer data-field="regularMarketDayLow" value="270.30">270.30</fin-streamer>
</body>
</html>
"""

MOCK_YAHOO_RESPONSE_INVALID = """
<!DOCTYPE html>
<html>
<body>
    <div>No data available</div>
</body>
</html>
"""

MOCK_YAHOO_RESPONSE_PARTIAL = """
<!DOCTYPE html>
<html>
<body>
    <fin-streamer data-field="regularMarketPrice" value="150.25">150.25</fin-streamer>
    <fin-streamer data-field="regularMarketChange" value="N/A">N/A</fin-streamer>
    <fin-streamer data-field="regularMarketVolume" value="--">--</fin-streamer>
</body>
</html>
"""

# Expected parsed data for mock responses
EXPECTED_STOCK_DATA_COMPLETE = {
    'symbol': 'AAPL',
    'price': Decimal('150.25'),
    'daily_change_percent': Decimal('1.45'),
    'daily_change_nominal': Decimal('2.15'),
    'volume': 45123456,
    'high': Decimal('152.10'),
    'low': Decimal('148.50'),
    'market': 'NASDAQ'
}

EXPECTED_STOCK_DATA_MINIMAL = {
    'symbol': 'AAPL',
    'price': Decimal('150.25'),
    'daily_change_percent': Decimal('0'),
    'daily_change_nominal': Decimal('0'),
    'volume': 0,
    'high': Decimal('150.25'),
    'low': Decimal('150.25'),
    'market': 'NASDAQ'
}

EXPECTED_STOCK_DATA_NEGATIVE = {
    'symbol': 'AAPL',
    'price': Decimal('148.50'),
    'daily_change_percent': Decimal('-1.17'),
    'daily_change_nominal': Decimal('-1.75'),
    'volume': 32456789,
    'high': Decimal('150.25'),
    'low': Decimal('147.80'),
    'market': 'NASDAQ'
}

# Mock DynamoDB responses
MOCK_DYNAMODB_ITEM = {
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

MOCK_DYNAMODB_ITEMS = [
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
    },
    {
        'symbol': 'MSFT',
        'price': Decimal('375.80'),
        'daily_change_percent': Decimal('0.85'),
        'daily_change_nominal': Decimal('3.18'),
        'volume': 23456789,
        'high': Decimal('378.50'),
        'low': Decimal('372.10'),
        'last_updated': '2024-01-01T12:00:00Z',
        'market': 'NASDAQ'
    },
    {
        'symbol': 'GOOGL',
        'price': Decimal('142.35'),
        'daily_change_percent': Decimal('-0.65'),
        'daily_change_nominal': Decimal('-0.93'),
        'volume': 18765432,
        'high': Decimal('144.20'),
        'low': Decimal('141.80'),
        'last_updated': '2024-01-01T12:00:00Z',
        'market': 'NASDAQ'
    }
]

MOCK_TABLE_DESCRIPTION = {
    'Table': {
        'TableName': 'test_nasdaq_stocks',
        'TableStatus': 'ACTIVE',
        'TableSizeBytes': 2048,
        'ItemCount': 100,
        'CreationDateTime': datetime(2024, 1, 1, 12, 0, 0),
        'BillingModeSummary': {
            'BillingMode': 'PAY_PER_REQUEST'
        },
        'AttributeDefinitions': [
            {
                'AttributeName': 'symbol',
                'AttributeType': 'S'
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'symbol',
                'KeyType': 'HASH'
            }
        ]
    }
}

# Test symbols lists
TEST_SYMBOLS_SMALL = ['AAPL', 'MSFT', 'GOOGL']
TEST_SYMBOLS_MEDIUM = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']
TEST_SYMBOLS_WITH_INVALID = ['AAPL', 'MSFT', 'INVALID', 'GOOGL', 'BADSTOCK']

# Mock symbols file content
MOCK_SYMBOLS_FILE_CONTENT = {
    "description": "Test NASDAQ symbols",
    "last_updated": "2024-01-01T12:00:00Z",
    "count": 5,
    "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
}

MOCK_SYMBOLS_FILE_SIMPLE_LIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

MOCK_SYMBOLS_FILE_ALTERNATIVE_FORMAT = {
    "data": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
}

# HTTP response status codes for testing
HTTP_STATUS_CODES = {
    'success': 200,
    'not_found': 404,
    'rate_limited': 429,
    'server_error': 500,
    'bad_gateway': 502,
    'service_unavailable': 503
}

# Mock request headers
MOCK_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; NASDAQScraper/1.0)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

# Error messages for testing
ERROR_MESSAGES = {
    'network_timeout': 'Request timeout',
    'connection_error': 'Connection failed',
    'parsing_error': 'Failed to parse response',
    'validation_error': 'Invalid data format',
    'rate_limit_error': 'Rate limit exceeded',
    'symbol_not_found': 'Symbol not found',
    'database_error': 'Database operation failed'
}

# Performance test data
PERFORMANCE_TEST_DATA = {
    'request_times': [0.1, 0.2, 0.15, 0.3, 0.25, 0.18, 0.22],
    'success_rates': [98.5, 99.1, 97.8, 99.5, 98.9],
    'memory_usage': [45.2, 47.1, 46.8, 48.3, 46.5],  # MB
    'cpu_usage': [2.5, 3.1, 2.8, 3.5, 2.9]  # percent
}

# Health check test data
HEALTH_CHECK_RESPONSES = {
    'healthy': {
        'status': 'healthy',
        'database_connection': True,
        'internet_connection': True,
        'memory_usage_mb': 45.2,
        'disk_space_gb': 125.8,
        'timestamp': '2024-01-01T12:00:00Z'
    },
    'unhealthy_database': {
        'status': 'unhealthy',
        'database_connection': False,
        'internet_connection': True,
        'memory_usage_mb': 45.2,
        'disk_space_gb': 125.8,
        'timestamp': '2024-01-01T12:00:00Z'
    },
    'unhealthy_network': {
        'status': 'unhealthy',
        'database_connection': True,
        'internet_connection': False,
        'memory_usage_mb': 45.2,
        'disk_space_gb': 125.8,
        'timestamp': '2024-01-01T12:00:00Z'
    }
}

# Configuration test values
TEST_CONFIG_VALUES = {
    'debug_mode': {
        'DEBUG': True,
        'LOG_LEVEL': 'DEBUG',
        'SCRAPE_INTERVAL': 300,
        'MAX_SYMBOLS_PER_BATCH': 5
    },
    'production_mode': {
        'DEBUG': False,
        'LOG_LEVEL': 'INFO',
        'SCRAPE_INTERVAL': 60,
        'MAX_SYMBOLS_PER_BATCH': 100
    },
    'invalid_config': {
        'SCRAPE_INTERVAL': 10,  # Too low
        'REQUEST_TIMEOUT': 2,   # Too low
        'MAX_RETRIES': 0        # Too low
    }
}

# Utility functions for test data generation
def generate_mock_stock_data(symbol: str, base_price: float = 100.0) -> Dict[str, Any]:
    """Generate mock stock data for testing."""
    price = Decimal(str(base_price))
    return {
        'symbol': symbol,
        'price': price,
        'daily_change_percent': Decimal('1.5'),
        'daily_change_nominal': price * Decimal('0.015'),
        'volume': 1000000,
        'high': price + Decimal('5.0'),
        'low': price - Decimal('3.0'),
        'last_updated': datetime.utcnow().isoformat() + 'Z',
        'market': 'NASDAQ'
    }


def generate_mock_batch_results(symbols: List[str], success_rate: float = 0.9) -> List[Dict[str, Any]]:
    """Generate mock batch results for testing."""
    results = []
    successful_count = int(len(symbols) * success_rate)
    
    for i, symbol in enumerate(symbols):
        if i < successful_count:
            results.append({
                'symbol': symbol,
                'success': True,
                'data': generate_mock_stock_data(symbol),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        else:
            results.append({
                'symbol': symbol,
                'success': False,
                'error': 'Mock error for testing',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
    
    return results


def get_mock_yahoo_response(scenario: str = 'complete') -> str:
    """Get mock Yahoo Finance response for different scenarios."""
    responses = {
        'complete': MOCK_YAHOO_RESPONSE_COMPLETE,
        'minimal': MOCK_YAHOO_RESPONSE_MINIMAL,
        'alternative': MOCK_YAHOO_RESPONSE_ALTERNATIVE_SELECTORS,
        'negative': MOCK_YAHOO_RESPONSE_NEGATIVE_CHANGE,
        'high_volume': MOCK_YAHOO_RESPONSE_HIGH_VOLUME,
        'invalid': MOCK_YAHOO_RESPONSE_INVALID,
        'partial': MOCK_YAHOO_RESPONSE_PARTIAL
    }
    return responses.get(scenario, MOCK_YAHOO_RESPONSE_COMPLETE)


def get_expected_stock_data(scenario: str = 'complete') -> Dict[str, Any]:
    """Get expected parsed stock data for different scenarios."""
    expected_data = {
        'complete': EXPECTED_STOCK_DATA_COMPLETE,
        'minimal': EXPECTED_STOCK_DATA_MINIMAL,
        'negative': EXPECTED_STOCK_DATA_NEGATIVE
    }
    return expected_data.get(scenario, EXPECTED_STOCK_DATA_COMPLETE)


def create_mock_response(status_code: int = 200, content: str = None, headers: Dict = None):
    """Create a mock HTTP response object."""
    from unittest.mock import Mock
    
    response = Mock()
    response.status_code = status_code
    response.content = (content or MOCK_YAHOO_RESPONSE_COMPLETE).encode('utf-8')
    response.text = content or MOCK_YAHOO_RESPONSE_COMPLETE
    response.headers = headers or {}
    response.elapsed.total_seconds.return_value = 0.5
    response.raise_for_status = Mock()
    
    if status_code >= 400:
        from requests.exceptions import HTTPError
        response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}")
    
    return response


def create_mock_dynamodb_responses():
    """Create comprehensive mock DynamoDB responses."""
    return {
        'put_item_success': {
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'test-request-id'
            }
        },
        'get_item_found': {
            'Item': MOCK_DYNAMODB_ITEM,
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        },
        'get_item_not_found': {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        },
        'scan_response': {
            'Items': MOCK_DYNAMODB_ITEMS,
            'Count': len(MOCK_DYNAMODB_ITEMS),
            'ScannedCount': len(MOCK_DYNAMODB_ITEMS),
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        },
        'batch_get_response': {
            'Responses': {
                'test_nasdaq_stocks': MOCK_DYNAMODB_ITEMS
            },
            'UnprocessedKeys': {},
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        },
        'describe_table_response': MOCK_TABLE_DESCRIPTION,
        'create_table_response': {
            'TableDescription': MOCK_TABLE_DESCRIPTION['Table']
        }
    }


# Test data for edge cases
EDGE_CASE_DATA = {
    'very_high_price': {
        'symbol': 'EXPENSIVE',
        'price': '9999.99',
        'volume': '1'
    },
    'very_low_price': {
        'symbol': 'PENNY',
        'price': '0.01',
        'volume': '1000000000'
    },
    'zero_volume': {
        'symbol': 'NOLIQUIDITY',
        'price': '100.00',
        'volume': '0'
    },
    'special_characters': {
        'symbol': 'BRK.A',  # Berkshire Hathaway Class A
        'price': '500000.00',
        'volume': '10'
    },
    'percentage_formats': [
        '+1.45%', '-2.33%', '0.00%', '+15.67%', '-0.01%'
    ],
    'volume_formats': [
        '45,123,456', '1.5M', '2.3B', '789K', '1,234', '0'
    ],
    'price_formats': [
        '$150.25', '150.25', '$1,500.00', '1,500.00', '$0.01'
    ]
}

# Rate limiting test scenarios
RATE_LIMITING_SCENARIOS = {
    'normal_load': {
        'requests_per_minute': 30,
        'expected_delays': [0, 0, 0, 2.0, 2.0]
    },
    'heavy_load': {
        'requests_per_minute': 60,
        'expected_delays': [0, 0, 30.0, 30.0, 30.0]
    },
    'burst_traffic': {
        'requests_per_minute': 100,
        'expected_delays': [0, 60.0, 60.0, 60.0, 60.0]
    }
}

# Error simulation data
ERROR_SIMULATION = {
    'network_errors': [
        'Connection timeout',
        'DNS resolution failed',
        'Connection refused',
        'SSL handshake failed'
    ],
    'http_errors': [
        {'status': 429, 'message': 'Too Many Requests'},
        {'status': 500, 'message': 'Internal Server Error'},
        {'status': 502, 'message': 'Bad Gateway'},
        {'status': 503, 'message': 'Service Unavailable'}
    ],
    'parsing_errors': [
        'Invalid HTML structure',
        'Missing price element',
        'Malformed data attributes',
        'Unexpected response format'
    ],
    'database_errors': [
        'Table does not exist',
        'Access denied',
        'Provisioned throughput exceeded',
        'Validation exception'
    ]
}

# Load testing data
LOAD_TEST_SCENARIOS = {
    'light_load': {
        'concurrent_scrapers': 1,
        'symbols_per_scraper': 10,
        'expected_duration': 30
    },
    'medium_load': {
        'concurrent_scrapers': 3,
        'symbols_per_scraper': 25,
        'expected_duration': 45
    },
    'heavy_load': {
        'concurrent_scrapers': 5,
        'symbols_per_scraper': 50,
        'expected_duration': 90
    }
}

# Validation test cases
VALIDATION_TEST_CASES = {
    'valid_symbols': ['AAPL', 'MSFT', 'GOOGL', 'META', 'TSLA'],
    'invalid_symbols': ['', '123', 'TOOLONGSTOCK', 'INVALID!', None],
    'valid_prices': [0.01, 1.0, 100.0, 1000.0, 9999.99],
    'invalid_prices': [-1.0, 0.0, 'invalid', None, float('inf')],
    'valid_volumes': [0, 1, 1000, 1000000, 999999999],
    'invalid_volumes': [-1, 'invalid', None, float('inf')],
    'valid_percentages': [-10.0, -1.5, 0.0, 1.5, 10.0, 15.67],
    'invalid_percentages': ['invalid', None, float('inf'), float('-inf')]
}

# Mock AWS credentials for testing
MOCK_AWS_CREDENTIALS = {
    'aws_access_key_id': 'AKIATEST12345',
    'aws_secret_access_key': 'test-secret-key',
    'region_name': 'us-east-1'
}

# System resource test data
SYSTEM_RESOURCE_DATA = {
    'memory_usage': {
        'low': 32.5,    # MB
        'normal': 64.2,  # MB
        'high': 128.7,   # MB
        'critical': 256.0 # MB
    },
    'disk_space': {
        'low': 5.2,      # GB
        'normal': 25.8,  # GB
        'high': 100.5,   # GB
        'abundant': 500.0 # GB
    },
    'cpu_usage': {
        'idle': 2.1,     # %
        'normal': 15.3,  # %
        'busy': 45.7,    # %
        'overloaded': 85.2 # %
    }
}

# Time-based test scenarios
TIME_SCENARIOS = {
    'market_hours': {
        'start': '09:30:00',
        'end': '16:00:00',
        'timezone': 'US/Eastern'
    },
    'pre_market': {
        'start': '04:00:00',
        'end': '09:30:00',
        'timezone': 'US/Eastern'
    },
    'after_hours': {
        'start': '16:00:00',
        'end': '20:00:00',
        'timezone': 'US/Eastern'
    },
    'weekend': {
        'days': ['Saturday', 'Sunday']
    },
    'holidays': [
        '2024-01-01',  # New Year's Day
        '2024-07-04',  # Independence Day
        '2024-12-25'   # Christmas Day
    ]
}