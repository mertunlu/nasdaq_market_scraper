"""Data models and schemas for the NASDAQ-100 scraper."""

from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any
import json


@dataclass
class StockData:
    """Data model for stock information."""
    
    symbol: str
    price: Decimal
    daily_change_percent: Decimal
    daily_change_nominal: Decimal
    volume: int
    high: Decimal
    low: Decimal
    open: Decimal                # NEW FIELD
    previous_close: Decimal      # NEW FIELD
    last_updated: str
    market: str = "NASDAQ"
    
    def __post_init__(self):
        """Validate data after initialization."""
        # Ensure symbol is uppercase
        self.symbol = self.symbol.upper()
        
        # Ensure timestamp is ISO format
        if isinstance(self.last_updated, datetime):
            self.last_updated = self.last_updated.isoformat() + 'Z'
        
        # Convert to Decimal for financial precision
        if not isinstance(self.price, Decimal):
            self.price = Decimal(str(self.price))
        if not isinstance(self.daily_change_percent, Decimal):
            self.daily_change_percent = Decimal(str(self.daily_change_percent))
        if not isinstance(self.daily_change_nominal, Decimal):
            self.daily_change_nominal = Decimal(str(self.daily_change_nominal))
        if not isinstance(self.high, Decimal):
            self.high = Decimal(str(self.high))
        if not isinstance(self.low, Decimal):
            self.low = Decimal(str(self.low))
        if not isinstance(self.open, Decimal):
            self.open = Decimal(str(self.open))
        if not isinstance(self.previous_close, Decimal):
            self.previous_close = Decimal(str(self.previous_close))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DynamoDB storage."""
        return {
            'symbol': self.symbol,
            'price': self.price,
            'daily_change_percent': self.daily_change_percent,
            'daily_change_nominal': self.daily_change_nominal,
            'volume': self.volume,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'previous_close': self.previous_close,
            'last_updated': self.last_updated,
            'market': self.market
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        # Convert Decimal to float for JSON serialization
        data = self.to_dict()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
        return json.dumps(data, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StockData':
        """Create StockData from dictionary."""
        return cls(
            symbol=data['symbol'],
            price=Decimal(str(data['price'])),
            daily_change_percent=Decimal(str(data['daily_change_percent'])),
            daily_change_nominal=Decimal(str(data['daily_change_nominal'])),
            volume=int(data['volume']),
            high=Decimal(str(data['high'])),
            low=Decimal(str(data['low'])),
            open=Decimal(str(data['open'])),
            previous_close=Decimal(str(data['previous_close'])),
            last_updated=data['last_updated'],
            market=data.get('market', 'NASDAQ')
        )
    
    def validate(self, min_price: float = 0.01, max_price: float = 10000.0) -> bool:
        """Validate stock data integrity."""
        try:
            # Check price bounds
            if self.price < Decimal(str(min_price)) or self.price > Decimal(str(max_price)):
                return False
            
            # Check that high >= low >= 0
            if self.high < self.low or self.low < 0:
                return False
            
            # Check that price is within daily range
            if self.price < self.low or self.price > self.high:
                return False
            
            # Check volume is non-negative
            if self.volume < 0:
                return False
            
            # Check symbol format (basic validation)
            if not self.symbol or len(self.symbol) < 1 or len(self.symbol) > 10:
                return False
            
            # Check open and previous_close are positive
            if self.open < 0 or self.previous_close < 0:
                return False
            
            return True
            
        except (TypeError, ValueError, AttributeError):
            return False


@dataclass
class ScrapingResult:
    """Result of a scraping operation."""
    
    symbol: str
    success: bool
    data: Optional[StockData] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat() + 'Z'


@dataclass
class HealthStatus:
    """Health check status model."""
    
    status: str  # 'healthy' or 'unhealthy'
    timestamp: str
    database_connection: bool
    internet_connection: bool
    memory_usage_mb: float
    disk_space_gb: float
    last_successful_scrape: Optional[str] = None
    error_rate_percent: float = 0.0
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat() + 'Z'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass 
class BatchResult:
    """Result of batch scraping operation."""
    
    total_symbols: int
    successful: int
    failed: int
    results: list[ScrapingResult]
    start_time: str
    end_time: str
    duration_seconds: float
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_symbols == 0:
            return 0.0
        return (self.successful / self.total_symbols) * 100
    
    def get_failed_symbols(self) -> list[str]:
        """Get list of symbols that failed to scrape."""
        return [result.symbol for result in self.results if not result.success]
    
    def get_successful_data(self) -> list[StockData]:
        """Get list of successfully scraped stock data."""
        return [result.data for result in self.results if result.success and result.data]


# Type aliases for clarity
SymbolList = list[str]
StockDataDict = Dict[str, StockData]
ScrapingResults = Dict[str, ScrapingResult]