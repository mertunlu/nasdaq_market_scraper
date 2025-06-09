"""Test suite for NASDAQ-100 scraper.

This package contains comprehensive tests for all components of the NASDAQ-100
financial data scraper, including unit tests, integration tests, and end-to-end
testing scenarios.

Test Structure:
- conftest.py: Shared pytest fixtures and configuration
- test_scraper.py: Tests for scraping functionality
- test_database.py: Tests for DynamoDB operations
- test_integration.py: End-to-end integration tests
- fixtures.py: Test data and mock responses

Running Tests:
    # Run all tests
    pytest tests/
    
    # Run with coverage
    pytest tests/ --cov=src --cov-report=html
    
    # Run specific test file
    pytest tests/test_scraper.py -v
    
    # Run integration tests only
    pytest tests/test_integration.py -v
"""

import os
import sys

# Add src directory to Python path for testing
test_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, 'src')

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Test configuration
TEST_CONFIG = {
    'test_symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
    'mock_aws_region': 'us-east-1',
    'mock_table_name': 'test_nasdaq_stocks',
    'test_timeout': 30,  # seconds
    'test_rate_limit': 5,  # requests per minute for testing
}