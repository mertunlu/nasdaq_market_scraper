"""Unit tests for the NASDAQ-100 scraper functionality."""

import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal
from datetime import datetime

from src.scraper import YahooFinanceScraper, NasdaqScraper
from src.models import StockData, ScrapingResult, BatchResult
from src.exceptions import (
    NetworkError, ParsingError, RateLimitError, 
    TimeoutError, SymbolNotFoundError
)
from src.utils import RateLimiter
from tests.fixtures import (
    get_mock_yahoo_response, get_expected_stock_data,
    create_mock_response, TEST_SYMBOLS_SMALL,
    ERROR_SIMULATION, HTTP_STATUS_CODES
)


class TestYahooFinanceScraper:
    """Test cases for YahooFinanceScraper class."""
    
    def test_scraper_initialization(self):
        """Test scraper initialization."""
        scraper = YahooFinanceScraper()
        
        assert scraper.base_url == "https://finance.yahoo.com/quote/"
        assert isinstance(scraper.rate_limiter, RateLimiter)
        assert scraper.session is not None
        assert scraper.stats['requests_made'] == 0
        assert scraper.stats['successful_scrapes'] == 0
    
    def test_scraper_initialization_with_custom_rate_limiter(self):
        """Test scraper initialization with custom rate limiter."""
        custom_limiter = RateLimiter(max_requests=10, time_window=30)
        scraper = YahooFinanceScraper(rate_limiter=custom_limiter)
        
        assert scraper.rate_limiter == custom_limiter
    
    @patch('requests.Session.get')
    def test_successful_scrape_complete_data(self, mock_get):
        """Test successful scraping with complete data."""
        # Setup mock response
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('AAPL')
        
        assert result is not None
        assert isinstance(result, StockData)
        assert result.symbol == 'AAPL'
        assert result.price == Decimal('150.25')
        assert result.daily_change_percent == Decimal('1.45')
        assert result.daily_change_nominal == Decimal('2.15')
        assert result.volume == 45123456
        assert result.high == Decimal('152.10')
        assert result.low == Decimal('148.50')
        assert result.market == 'NASDAQ'
        
        # Verify stats
        assert scraper.stats['requests_made'] == 1
        assert scraper.stats['successful_scrapes'] == 1
        assert scraper.stats['failed_scrapes'] == 0
    
    @patch('requests.Session.get')
    def test_successful_scrape_minimal_data(self, mock_get):
        """Test successful scraping with minimal data."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('minimal')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('AAPL')
        
        assert result is not None
        assert result.symbol == 'AAPL'
        assert result.price == Decimal('150.25')
        # Should have defaults for missing data
        assert result.daily_change_percent == Decimal('0')
        assert result.daily_change_nominal == Decimal('0')
        assert result.volume == 0
        assert result.high == Decimal('150.25')  # Should equal price
        assert result.low == Decimal('150.25')   # Should equal price
    
    @patch('requests.Session.get')
    def test_scrape_negative_change(self, mock_get):
        """Test scraping stock with negative daily change."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('negative')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('AAPL')
        
        assert result is not None
        assert result.price == Decimal('148.50')
        assert result.daily_change_percent == Decimal('-1.17')
        assert result.daily_change_nominal == Decimal('-1.75')
    
    @patch('requests.Session.get')
    def test_scrape_high_volume_stock(self, mock_get):
        """Test scraping stock with high volume."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('high_volume')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('TSLA')
        
        assert result is not None
        assert result.volume > 100000000  # Should parse volume abbreviations
    
    def test_scrape_invalid_symbol(self):
        """Test scraping with invalid symbol."""
        scraper = YahooFinanceScraper()
        
        with pytest.raises(ValueError):
            scraper.scrape_symbol('')
        
        with pytest.raises(ValueError):
            scraper.scrape_symbol(None)
    
    @patch('requests.Session.get')
    def test_scrape_symbol_not_found(self, mock_get):
        """Test scraping non-existent symbol."""
        mock_response = create_mock_response(status_code=404)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(SymbolNotFoundError):
            scraper.scrape_symbol('INVALID')
        
        assert scraper.stats['failed_scrapes'] == 1
    
    @patch('requests.Session.get')
    def test_rate_limit_error(self, mock_get):
        """Test handling of rate limit responses."""
        mock_response = create_mock_response(status_code=429)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(RateLimitError):
            scraper.scrape_symbol('AAPL')
    
    @patch('requests.Session.get')
    def test_server_error(self, mock_get):
        """Test handling of server errors."""
        mock_response = create_mock_response(status_code=500)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(NetworkError):
            scraper.scrape_symbol('AAPL')
    
    @patch('requests.Session.get')
    def test_timeout_error(self, mock_get):
        """Test handling of request timeouts."""
        mock_get.side_effect = requests.exceptions.Timeout()
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(TimeoutError):
            scraper.scraper_symbol('AAPL')
        
        assert scraper.stats['timeout_errors'] == 1
    
    @patch('requests.Session.get')
    def test_connection_error(self, mock_get):
        """Test handling of connection errors."""
        mock_get.side_effect = requests.exceptions.ConnectionError()
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(NetworkError):
            scraper.scrape_symbol('AAPL')
    
    @patch('requests.Session.get')
    def test_parsing_error_invalid_html(self, mock_get):
        """Test handling of parsing errors with invalid HTML."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('invalid')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(ParsingError):
            scraper.scrape_symbol('AAPL')
        
        assert scraper.stats['parsing_errors'] == 1
    
    @patch('requests.Session.get')
    def test_user_agent_rotation(self, mock_get):
        """Test user agent rotation functionality."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        # Make multiple requests to trigger user agent rotation
        for i in range(12):  # Should trigger rotation after 10 requests
            scraper.scrape_symbol('AAPL')
        
        # Verify multiple calls were made
        assert mock_get.call_count == 12
    
    @patch('requests.Session.get')
    def test_request_delay(self, mock_get):
        """Test request delay functionality."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with patch('time.sleep') as mock_sleep:
            scraper.scrape_symbol('AAPL')
            mock_sleep.assert_called()
    
    @patch('requests.Session.get')
    def test_batch_scraping_success(self, mock_get):
        """Test successful batch scraping."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        symbols = TEST_SYMBOLS_SMALL
        
        result = scraper.scrape_batch(symbols)
        
        assert isinstance(result, BatchResult)
        assert result.total_symbols == len(symbols)
        assert result.successful == len(symbols)
        assert result.failed == 0
        assert result.success_rate == 100.0
        assert len(result.results) == len(symbols)
        
        # Verify all results are successful
        for scraping_result in result.results:
            assert scraping_result.success is True
            assert scraping_result.data is not None
    
    @patch('requests.Session.get')
    def test_batch_scraping_mixed_results(self, mock_get):
        """Test batch scraping with mixed success/failure results."""
        def mock_response_side_effect(url, **kwargs):
            if 'AAPL' in url:
                return create_mock_response(200, get_mock_yahoo_response('complete'))
            elif 'MSFT' in url:
                return create_mock_response(200, get_mock_yahoo_response('complete'))
            else:
                return create_mock_response(404)
        
        mock_get.side_effect = mock_response_side_effect
        
        scraper = YahooFinanceScraper()
        symbols = ['AAPL', 'MSFT', 'INVALID']
        
        result = scraper.scrape_batch(symbols)
        
        assert result.total_symbols == 3
        assert result.successful == 2
        assert result.failed == 1
        assert result.success_rate == pytest.approx(66.67, rel=1e-2)
        
        failed_symbols = result.get_failed_symbols()
        assert 'INVALID' in failed_symbols
    
    def test_get_stats(self):
        """Test statistics retrieval."""
        scraper = YahooFinanceScraper()
        stats = scraper.get_stats()
        
        assert 'requests_made' in stats
        assert 'successful_scrapes' in stats
        assert 'failed_scrapes' in stats
        assert 'success_rate_percent' in stats
        assert 'timestamp' in stats
        
        # Initial stats should be zero
        assert stats['requests_made'] == 0
        assert stats['successful_scrapes'] == 0
        assert stats['success_rate_percent'] == 0
    
    def test_reset_stats(self):
        """Test statistics reset functionality."""
        scraper = YahooFinanceScraper()
        
        # Manually set some stats
        scraper.stats['requests_made'] = 10
        scraper.stats['successful_scrapes'] = 8
        
        scraper.reset_stats()
        
        assert scraper.stats['requests_made'] == 0
        assert scraper.stats['successful_scrapes'] == 0
    
    def test_close_scraper(self):
        """Test scraper cleanup."""
        scraper = YahooFinanceScraper()
        
        # Should not raise an exception
        scraper.close()
        
        # Session should be closed
        assert hasattr(scraper, 'session')


class TestNasdaqScraper:
    """Test cases for NasdaqScraper class."""
    
    @patch('src.utils.load_nasdaq_symbols')
    def test_nasdaq_scraper_initialization(self, mock_load_symbols):
        """Test NASDAQ scraper initialization."""
        mock_load_symbols.return_value = TEST_SYMBOLS_SMALL
        
        scraper = NasdaqScraper()
        
        assert len(scraper.symbols) == len(TEST_SYMBOLS_SMALL)
        assert isinstance(scraper.scraper, YahooFinanceScraper)
        assert isinstance(scraper.rate_limiter, RateLimiter)
    
    @patch('src.utils.load_nasdaq_symbols')
    def test_nasdaq_scraper_debug_mode(self, mock_load_symbols):
        """Test NASDAQ scraper in debug mode."""
        mock_load_symbols.return_value = ['AAPL'] * 50  # 50 symbols
        
        scraper = NasdaqScraper(debug=True)
        
        # Should limit symbols in debug mode
        assert len(scraper.symbols) <= 5  # MAX_SYMBOLS_PER_BATCH in debug
    
    @patch('src.utils.load_nasdaq_symbols')
    @patch('requests.Session.get')
    def test_scrape_all(self, mock_get, mock_load_symbols):
        """Test scraping all symbols."""
        mock_load_symbols.return_value = TEST_SYMBOLS_SMALL
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = NasdaqScraper()
        result = scraper.scrape_all()
        
        assert isinstance(result, BatchResult)
        assert result.total_symbols == len(TEST_SYMBOLS_SMALL)
        assert result.successful > 0
    
    @patch('src.utils.load_nasdaq_symbols')
    def test_get_symbols(self, mock_load_symbols):
        """Test getting symbols list."""
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        mock_load_symbols.return_value = test_symbols
        
        scraper = NasdaqScraper()
        symbols = scraper.get_symbols()
        
        assert symbols == test_symbols
        # Should return copy, not original
        symbols.append('EXTRA')
        assert len(scraper.symbols) == 3
    
    @patch('src.utils.load_nasdaq_symbols')
    def test_get_stats(self, mock_load_symbols):
        """Test getting scraper statistics."""
        mock_load_symbols.return_value = TEST_SYMBOLS_SMALL
        
        scraper = NasdaqScraper()
        stats = scraper.get_stats()
        
        assert isinstance(stats, dict)
        assert 'requests_made' in stats
        assert 'successful_scrapes' in stats
    
    @patch('src.utils.load_nasdaq_symbols')
    def test_close_nasdaq_scraper(self, mock_load_symbols):
        """Test NASDAQ scraper cleanup."""
        mock_load_symbols.return_value = TEST_SYMBOLS_SMALL
        
        scraper = NasdaqScraper()
        
        # Should not raise an exception
        scraper.close()


class TestDataValidation:
    """Test cases for data validation in scraping."""
    
    def test_stock_data_validation_valid(self, sample_stock_data):
        """Test validation of valid stock data."""
        assert sample_stock_data.validate() is True
    
    def test_stock_data_validation_invalid_price(self, sample_stock_data):
        """Test validation with invalid price."""
        sample_stock_data.price = Decimal('-1.0')
        assert sample_stock_data.validate() is False
        
        sample_stock_data.price = Decimal('0')
        assert sample_stock_data.validate() is False
    
    def test_stock_data_validation_invalid_range(self, sample_stock_data):
        """Test validation with invalid high/low range."""
        sample_stock_data.high = Decimal('100.0')
        sample_stock_data.low = Decimal('200.0')  # Low > High
        assert sample_stock_data.validate() is False
    
    def test_stock_data_validation_price_out_of_range(self, sample_stock_data):
        """Test validation with price outside daily range."""
        sample_stock_data.price = Decimal('300.0')  # Above high
        assert sample_stock_data.validate() is False
        
        sample_stock_data.price = Decimal('100.0')  # Below low
        assert sample_stock_data.validate() is False
    
    def test_stock_data_validation_negative_volume(self, sample_stock_data):
        """Test validation with negative volume."""
        sample_stock_data.volume = -1
        assert sample_stock_data.validate() is False


class TestErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.parametrize("error_type,expected_exception", [
        (requests.exceptions.Timeout, TimeoutError),
        (requests.exceptions.ConnectionError, NetworkError),
        (requests.exceptions.RequestException, NetworkError),
    ])
    @patch('requests.Session.get')
    def test_network_error_handling(self, mock_get, error_type, expected_exception):
        """Test handling of various network errors."""
        mock_get.side_effect = error_type()
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(expected_exception):
            scraper.scrape_symbol('AAPL')
    
    @pytest.mark.parametrize("status_code,expected_exception", [
        (404, SymbolNotFoundError),
        (429, RateLimitError),
        (500, NetworkError),
        (502, NetworkError),
        (503, NetworkError),
    ])
    @patch('requests.Session.get')
    def test_http_error_handling(self, mock_get, status_code, expected_exception):
        """Test handling of various HTTP status codes."""
        mock_response = create_mock_response(status_code=status_code)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(expected_exception):
            scraper.scrape_symbol('AAPL')


class TestRateLimiting:
    """Test cases for rate limiting functionality."""
    
    def test_rate_limiter_initialization(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(max_requests=10, time_window=60)
        
        assert limiter.max_requests == 10
        assert limiter.time_window == 60
        assert len(limiter.requests) == 0
    
    def test_rate_limiter_no_wait_needed(self):
        """Test rate limiter when no wait is needed."""
        limiter = RateLimiter(max_requests=10, time_window=60)
        
        wait_time = limiter.wait_if_needed()
        assert wait_time == 0.0
    
    @patch('time.sleep')
    @patch('time.time')
    def test_rate_limiter_wait_needed(self, mock_time, mock_sleep):
        """Test rate limiter when wait is needed."""
        # Mock time to simulate requests within time window
        mock_time.side_effect = [0, 1, 2, 3, 4, 5, 6]  # Simulate time progression
        
        limiter = RateLimiter(max_requests=3, time_window=10)
        
        # Make requests up to limit
        for _ in range(3):
            limiter.wait_if_needed()
        
        # Next request should trigger wait
        wait_time = limiter.wait_if_needed()
        
        # Should have called sleep
        mock_sleep.assert_called()
        assert wait_time > 0
    
    def test_rate_limiter_current_rate(self):
        """Test getting current request rate."""
        limiter = RateLimiter(max_requests=10, time_window=60)
        
        # Initially should be 0
        assert limiter.get_current_rate() == 0
        
        # After some requests
        limiter.wait_if_needed()
        limiter.wait_if_needed()
        
        rate = limiter.get_current_rate()
        assert rate >= 0


class TestPerformance:
    """Test cases for performance and load testing."""
    
    @patch('requests.Session.get')
    def test_scraper_performance_single_request(self, mock_get):
        """Test performance of single scraping request."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        import time
        start_time = time.time()
        result = scraper.scrape_symbol('AAPL')
        end_time = time.time()
        
        # Should complete reasonably quickly (less than 1 second in mocked environment)
        duration = end_time - start_time
        assert duration < 1.0
        assert result is not None
    
    @patch('requests.Session.get')
    def test_batch_scraping_performance(self, mock_get):
        """Test performance of batch scraping."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        symbols = TEST_SYMBOLS_SMALL * 2  # 6 symbols total
        
        import time
        start_time = time.time()
        result = scraper.scrape_batch(symbols)
        end_time = time.time()
        
        duration = end_time - start_time
        
        # Should complete all symbols
        assert result.total_symbols == len(symbols)
        assert result.successful == len(symbols)
        
        # Duration should be reasonable (with mocked delays)
        assert duration < 30.0  # 30 seconds max for 6 symbols
    
    def test_memory_usage_batch_scraping(self):
        """Test memory usage during batch scraping."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create scraper and simulate some operations
        scraper = YahooFinanceScraper()
        
        # Memory usage should not increase dramatically
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Should not use excessive memory (less than 50MB increase)
        assert memory_increase < 50


class TestEdgeCases:
    """Test cases for edge cases and unusual scenarios."""
    
    @patch('requests.Session.get')
    def test_empty_response_content(self, mock_get):
        """Test handling of empty response content."""
        mock_response = create_mock_response(status_code=200, content="")
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(ParsingError):
            scraper.scrape_symbol('AAPL')
    
    @patch('requests.Session.get')
    def test_malformed_html_response(self, mock_get):
        """Test handling of malformed HTML."""
        malformed_html = "<html><body><div>Incomplete HTML"
        mock_response = create_mock_response(status_code=200, content=malformed_html)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(ParsingError):
            scraper.scrape_symbol('AAPL')
    
    @patch('requests.Session.get')
    def test_partial_data_response(self, mock_get):
        """Test handling of response with partial data."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('partial')
        )
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('AAPL')
        
        # Should still work with minimal data
        assert result is not None
        assert result.symbol == 'AAPL'
        assert result.price == Decimal('150.25')
    
    def test_scrape_symbol_case_sensitivity(self):
        """Test symbol case handling."""
        scraper = YahooFinanceScraper()
        
        with patch.object(scraper, '_make_request') as mock_request:
            mock_request.return_value = create_mock_response(
                status_code=200,
                content=get_mock_yahoo_response('complete')
            )
            
            with patch.object(scraper, '_parse_response') as mock_parse:
                mock_parse.return_value = StockData(
                    symbol='AAPL',
                    price=Decimal('150.25'),
                    daily_change_percent=Decimal('1.45'),
                    daily_change_nominal=Decimal('2.15'),
                    volume=45123456,
                    high=Decimal('152.10'),
                    low=Decimal('148.50'),
                    last_updated='2024-01-01T12:00:00Z'
                )
                
                # Test lowercase input
                result = scraper.scrape_symbol('aapl')
                assert result.symbol == 'AAPL'  # Should be uppercase
    
    @patch('requests.Session.get')
    def test_very_large_numbers(self, mock_get):
        """Test handling of very large financial numbers."""
        large_number_html = """
        <html><body>
            <fin-streamer data-field="regularMarketPrice" value="999999.99">999,999.99</fin-streamer>
            <fin-streamer data-field="regularMarketVolume" value="999999999">999,999,999</fin-streamer>
        </body></html>
        """
        
        mock_response = create_mock_response(status_code=200, content=large_number_html)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('EXPENSIVE')
        
        # Should handle large numbers correctly
        assert result is not None
        assert result.price == Decimal('999999.99')
        assert result.volume == 999999999
    
    @patch('requests.Session.get')
    def test_zero_values(self, mock_get):
        """Test handling of zero values."""
        zero_values_html = """
        <html><body>
            <fin-streamer data-field="regularMarketPrice" value="0.01">0.01</fin-streamer>
            <fin-streamer data-field="regularMarketChange" value="0.00">0.00</fin-streamer>
            <fin-streamer data-field="regularMarketVolume" value="0">0</fin-streamer>
        </body></html>
        """
        
        mock_response = create_mock_response(status_code=200, content=zero_values_html)
        mock_get.return_value = mock_response
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('LOWPRICE')
        
        assert result is not None
        assert result.price == Decimal('0.01')
        assert result.daily_change_nominal == Decimal('0.00')
        assert result.volume == 0
    
    def test_batch_scraping_empty_list(self):
        """Test batch scraping with empty symbol list."""
        scraper = YahooFinanceScraper()
        result = scraper.scrape_batch([])
        
        assert isinstance(result, BatchResult)
        assert result.total_symbols == 0
        assert result.successful == 0
        assert result.failed == 0
        assert len(result.results) == 0
    
    @patch('requests.Session.get')
    def test_concurrent_scraping_simulation(self, mock_get):
        """Test simulation of concurrent scraping behavior."""
        mock_response = create_mock_response(
            status_code=200,
            content=get_mock_yahoo_response('complete')
        )
        mock_get.return_value = mock_response
        
        # Create multiple scrapers to simulate concurrent usage
        scrapers = [YahooFinanceScraper() for _ in range(3)]
        
        results = []
        for scraper in scrapers:
            result = scraper.scrape_symbol('AAPL')
            results.append(result)
        
        # All should succeed
        assert all(result is not None for result in results)
        assert all(result.symbol == 'AAPL' for result in results)


class TestRetryMechanism:
    """Test cases for retry and backoff mechanisms."""
    
    @patch('requests.Session.get')
    @patch('time.sleep')
    def test_retry_on_server_error(self, mock_sleep, mock_get):
        """Test retry mechanism on server errors."""
        # First two calls fail, third succeeds
        mock_get.side_effect = [
            create_mock_response(status_code=500),
            create_mock_response(status_code=500),
            create_mock_response(status_code=200, content=get_mock_yahoo_response('complete'))
        ]
        
        scraper = YahooFinanceScraper()
        result = scraper.scrape_symbol('AAPL')
        
        # Should eventually succeed
        assert result is not None
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2  # Two retries
    
    @patch('requests.Session.get')
    def test_retry_exhaustion(self, mock_get):
        """Test behavior when all retries are exhausted."""
        # Always return server error
        mock_get.return_value = create_mock_response(status_code=500)
        
        scraper = YahooFinanceScraper()
        
        with pytest.raises(NetworkError):
            scraper.scrape_symbol('AAPL')
        
        # Should have made maximum retry attempts
        assert mock_get.call_count >= 3


class TestConfiguration:
    """Test cases for configuration handling."""
    
    def test_scraper_with_custom_config(self, mock_config):
        """Test scraper with custom configuration."""
        with patch('src.scraper.config', mock_config):
            scraper = YahooFinanceScraper()
            
            assert scraper.base_url == mock_config.YAHOO_FINANCE_BASE_URL
    
    def test_rate_limiter_with_config(self, mock_config):
        """Test rate limiter using configuration values."""
        rate_limiter = RateLimiter(
            max_requests=mock_config.RATE_LIMIT_REQUESTS,
            time_window=mock_config.RATE_LIMIT_WINDOW
        )
        
        assert rate_limiter.max_requests == mock_config.RATE_LIMIT_REQUESTS
        assert rate_limiter.time_window == mock_config.RATE_LIMIT_WINDOW


if __name__ == '__main__':
    pytest.main([__file__])