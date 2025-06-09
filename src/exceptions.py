"""Custom exceptions for the NASDAQ-100 scraper."""


class ScraperError(Exception):
    """Base exception for scraper errors."""
    
    def __init__(self, message: str, symbol: str = None, details: dict = None):
        super().__init__(message)
        self.symbol = symbol
        self.details = details or {}


class NetworkError(ScraperError):
    """Network-related errors during data scraping."""
    pass


class DataValidationError(ScraperError):
    """Data validation errors."""
    pass


class DatabaseError(ScraperError):
    """Database operation errors."""
    pass


class RateLimitError(NetworkError):
    """Rate limiting errors."""
    pass


class ParsingError(ScraperError):
    """HTML parsing errors."""
    pass


class ConfigurationError(ScraperError):
    """Configuration-related errors."""
    pass


class SymbolNotFoundError(ScraperError):
    """Symbol not found or invalid."""
    pass


class TimeoutError(NetworkError):
    """Request timeout errors."""
    pass


class AuthenticationError(DatabaseError):
    """AWS authentication errors."""
    pass