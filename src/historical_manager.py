"""Historical data management system for NASDAQ-100 stocks."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from src.historical_scraper import YahooHistoricalScraper, HistoricalDataValidator
from src.historical_database import HistoricalDataManager
from src.utils import load_nasdaq_symbols
from src.config import config


@dataclass
class HistoricalSyncResult:
    """Result of historical data synchronization."""
    
    symbol: str
    success: bool
    data_points_processed: int
    data_points_saved: int
    date_range: Tuple[str, str]
    error: Optional[str] = None
    duration_seconds: float = 0.0


class HistoricalDataOrchestrator:
    """Orchestrates historical data collection and storage."""
    
    def __init__(self, debug: bool = False):
        self.logger = logging.getLogger(__name__)
        self.debug = debug
        
        # Initialize components
        self.scraper = YahooHistoricalScraper(debug=debug)
        self.db_manager = HistoricalDataManager()
        self.validator = HistoricalDataValidator()
        
        # Statistics
        self.stats = {
            'total_symbols_processed': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_data_points': 0,
            'total_saved_points': 0,
            'start_time': None,
            'end_time': None
        }
    
    def initialize(self) -> bool:
        """Initialize the historical data system."""
        try:
            self.logger.info("Initializing historical data system...")
            
            # Create table if it doesn't exist
            if not self.db_manager.create_table_if_not_exists():
                self.logger.error("Failed to create or access historical data table")
                return False
            
            # Test scraper connectivity
            test_symbols = ['META']  # Test with one symbol
            test_data = self.scraper.scrape_historical_data(test_symbols[0], period='5d')
            
            if not test_data:
                self.logger.error("Failed to scrape test data")
                return False
            
            self.logger.info("Historical data system initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize historical data system: {e}")
            return False
    
    def sync_symbol_historical_data(
        self, 
        symbol: str, 
        period: str = "1y",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> HistoricalSyncResult:
        """Sync historical data for a single symbol."""
        
        start_time = datetime.utcnow()
        self.logger.info(f"Starting historical sync for {symbol}")
        
        try:
            # Check if we need to fetch data
            if not force_refresh and not start_date:
                latest_date = self.db_manager.get_latest_date_for_symbol(symbol)
                if latest_date:
                    # Only fetch recent data if we have historical data
                    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
                    if latest_date >= yesterday:
                        self.logger.info(f"Historical data for {symbol} is up to date")
                        return HistoricalSyncResult(
                            symbol=symbol,
                            success=True,
                            data_points_processed=0,
                            data_points_saved=0,
                            date_range=(latest_date, latest_date)
                        )
                    else:
                        # Fetch from latest date + 1 to today
                        start_date = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                        end_date = datetime.utcnow().strftime('%Y-%m-%d')
                        period = None  # Use date range instead
            
            # Scrape historical data
            if period and not start_date:
                historical_data = self.scraper.scrape_historical_data(symbol, period=period)
            else:
                historical_data = self.scraper.scrape_historical_data(
                    symbol, 
                    start_date=start_date, 
                    end_date=end_date
                )
            
            if not historical_data:
                return HistoricalSyncResult(
                    symbol=symbol,
                    success=False,
                    data_points_processed=0,
                    data_points_saved=0,
                    date_range=("", ""),
                    error="No historical data retrieved"
                )
            
            # Validate data
            valid_data, invalid_reasons = self.validator.validate_symbol_data(historical_data)
            
            if invalid_reasons:
                self.logger.warning(f"Found {len(invalid_reasons)} invalid data points for {symbol}")
                for reason in invalid_reasons[:5]:  # Log first 5 reasons
                    self.logger.warning(reason)
            
            if not valid_data:
                return HistoricalSyncResult(
                    symbol=symbol,
                    success=False,
                    data_points_processed=len(historical_data),
                    data_points_saved=0,
                    date_range=("", ""),
                    error="No valid data points after validation"
                )
            
            # Filter out existing data unless force refresh
            if not force_refresh:
                new_data = []
                for data_point in valid_data:
                    if not self.db_manager.check_data_exists(symbol, data_point.date):
                        new_data.append(data_point)
                valid_data = new_data
            
            if not valid_data:
                self.logger.info(f"No new data to save for {symbol}")
                return HistoricalSyncResult(
                    symbol=symbol,
                    success=True,
                    data_points_processed=len(historical_data),
                    data_points_saved=0,
                    date_range=("", "")
                )
            
            # Save to database
            saved_count, failed_items = self.db_manager.save_batch_historical_data(valid_data)
            
            # Calculate date range
            dates = [dp.date for dp in valid_data]
            date_range = (min(dates), max(dates)) if dates else ("", "")
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            success = saved_count > 0
            error_msg = None
            if failed_items:
                error_msg = f"Failed to save {len(failed_items)} items"
            
            result = HistoricalSyncResult(
                symbol=symbol,
                success=success,
                data_points_processed=len(historical_data),
                data_points_saved=saved_count,
                date_range=date_range,
                error=error_msg,
                duration_seconds=duration
            )
            
            if success:
                self.logger.info(
                    f"Successfully synced {symbol}: {saved_count} points saved "
                    f"({date_range[0]} to {date_range[1]}) in {duration:.1f}s"
                )
            else:
                self.logger.error(f"Failed to sync {symbol}: {error_msg}")
            
            return result
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.logger.error(f"Error syncing {symbol}: {e}")
            
            return HistoricalSyncResult(
                symbol=symbol,
                success=False,
                data_points_processed=0,
                data_points_saved=0,
                date_range=("", ""),
                error=str(e),
                duration_seconds=duration
            )
    
    def sync_all_symbols(
        self, 
        period: str = "1y", 
        symbols: Optional[List[str]] = None,
        force_refresh: bool = False,
        max_symbols: Optional[int] = None
    ) -> List[HistoricalSyncResult]:
        """Sync historical data for all NASDAQ-100 symbols."""
        
        self.stats['start_time'] = datetime.utcnow()
        
        # Load symbols
        if symbols is None:
            symbols = load_nasdaq_symbols()
        
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        self.logger.info(f"Starting historical sync for {len(symbols)} symbols (period: {period})")
        
        results = []
        self.stats['total_symbols_processed'] = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            self.logger.info(f"Processing {symbol} ({i}/{len(symbols)})")
            
            try:
                result = self.sync_symbol_historical_data(
                    symbol, 
                    period=period, 
                    force_refresh=force_refresh
                )
                results.append(result)
                
                # Update statistics
                if result.success:
                    self.stats['successful_symbols'] += 1
                    self.stats['total_saved_points'] += result.data_points_saved
                else:
                    self.stats['failed_symbols'] += 1
                
                self.stats['total_data_points'] += result.data_points_processed
                
                # Progress logging
                if i % 10 == 0 or i == len(symbols):
                    success_rate = (self.stats['successful_symbols'] / i) * 100
                    self.logger.info(f"Progress: {i}/{len(symbols)} symbols processed, {success_rate:.1f}% success rate")
                
            except Exception as e:
                self.logger.error(f"Fatal error processing {symbol}: {e}")
                results.append(HistoricalSyncResult(
                    symbol=symbol,
                    success=False,
                    data_points_processed=0,
                    data_points_saved=0,
                    date_range=("", ""),
                    error=str(e)
                ))
                self.stats['failed_symbols'] += 1
        
        self.stats['end_time'] = datetime.utcnow()
        
        # Log final results
        self._log_sync_summary(results)
        
        return results
    
    def sync_recent_data(self, days_back: int = 7) -> List[HistoricalSyncResult]:
        """Sync recent historical data (last N days) for all symbols."""
        
        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        self.logger.info(f"Syncing recent data from {start_date} to {end_date}")
        
        symbols = load_nasdaq_symbols()
        results = []
        
        for symbol in symbols:
            result = self.sync_symbol_historical_data(
                symbol,
                start_date=start_date,
                end_date=end_date,
                force_refresh=True  # Always refresh recent data
            )
            results.append(result)
        
        return results
    
    def create_daily_snapshot(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """Create daily snapshot from current data and save to historical table."""
        
        if target_date is None:
            target_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        self.logger.info(f"Creating daily snapshot for {target_date}")
        
        try:
            # This would integrate with your main real-time scraper
            from src.database import DynamoDBManager
            from src.models import StockData
            
            current_db = DynamoDBManager()
            current_stocks = current_db.get_all_stocks()
            
            snapshot_data = []
            
            for stock in current_stocks:
                # Convert current stock data to historical format
                historical_point = type('HistoricalDataPoint', (), {
                    'symbol': stock.symbol,
                    'date': target_date,
                    'open': stock.open,
                    'high': stock.high,
                    'low': stock.low,
                    'close': stock.price,  # Current price becomes close
                    'adj_close': stock.price,  # Assuming no adjustments for daily data
                    'volume': stock.volume,
                    'daily_change_percent': stock.daily_change_percent,
                    'daily_change_nominal': stock.daily_change_nominal
                })()
                
                snapshot_data.append(historical_point)
            
            # Save snapshot to historical table
            saved_count, failed_items = self.db_manager.save_batch_historical_data(snapshot_data)
            
            result = {
                'date': target_date,
                'total_symbols': len(current_stocks),
                'saved_count': saved_count,
                'failed_count': len(failed_items),
                'success': saved_count > 0,
                'failed_items': failed_items
            }
            
            self.logger.info(f"Daily snapshot completed: {saved_count}/{len(current_stocks)} symbols saved")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error creating daily snapshot: {e}")
            return {
                'date': target_date,
                'success': False,
                'error': str(e)
            }
    
    def get_symbol_coverage_report(self) -> Dict[str, Any]:
        """Generate a report on historical data coverage."""
        
        self.logger.info("Generating historical data coverage report")
        
        try:
            symbols = load_nasdaq_symbols()
            coverage_data = {}
            
            for symbol in symbols:
                latest_date = self.db_manager.get_latest_date_for_symbol(symbol)
                if latest_date:
                    # Get count of records
                    records = self.db_manager.get_symbol_historical_data(
                        symbol, 
                        limit=1000  # Just to get a count
                    )
                    
                    coverage_data[symbol] = {
                        'latest_date': latest_date,
                        'record_count': len(records),
                        'has_data': True
                    }
                else:
                    coverage_data[symbol] = {
                        'latest_date': None,
                        'record_count': 0,
                        'has_data': False
                    }
            
            # Calculate summary statistics
            symbols_with_data = len([s for s in coverage_data.values() if s['has_data']])
            total_records = sum(s['record_count'] for s in coverage_data.values())
            
            # Find date range
            latest_dates = [s['latest_date'] for s in coverage_data.values() if s['latest_date']]
            date_range = {
                'earliest': min(latest_dates) if latest_dates else None,
                'latest': max(latest_dates) if latest_dates else None
            }
            
            report = {
                'total_symbols': len(symbols),
                'symbols_with_data': symbols_with_data,
                'coverage_percentage': (symbols_with_data / len(symbols)) * 100,
                'total_historical_records': total_records,
                'date_range': date_range,
                'symbol_details': coverage_data,
                'generated_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            self.logger.info(
                f"Coverage report: {symbols_with_data}/{len(symbols)} symbols "
                f"({report['coverage_percentage']:.1f}%), {total_records} total records"
            )
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating coverage report: {e}")
            return {'error': str(e)}
    
    def _log_sync_summary(self, results: List[HistoricalSyncResult]):
        """Log summary of sync operation."""
        
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        self.logger.info("=" * 60)
        self.logger.info("HISTORICAL DATA SYNC SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total symbols processed: {len(results)}")
        self.logger.info(f"Successful: {len(successful)}")
        self.logger.info(f"Failed: {len(failed)}")
        self.logger.info(f"Success rate: {(len(successful) / len(results)) * 100:.1f}%")
        self.logger.info(f"Total data points saved: {sum(r.data_points_saved for r in results)}")
        self.logger.info(f"Total duration: {duration:.1f} seconds")
        
        if failed:
            self.logger.warning("Failed symbols:")
            for result in failed[:10]:  # Show first 10 failures
                self.logger.warning(f"  {result.symbol}: {result.error}")
            if len(failed) > 10:
                self.logger.warning(f"  ... and {len(failed) - 10} more")
        
        self.logger.info("=" * 60)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        stats = self.stats.copy()
        
        # Add scraper stats
        scraper_stats = self.scraper.get_stats()
        stats.update({
            'scraper_requests': scraper_stats['requests_made'],
            'scraper_success_rate': scraper_stats['success_rate_percent']
        })
        
        return stats
    
    def close(self):
        """Clean up resources."""
        if self.scraper:
            self.scraper.close()


# Utility functions for common operations

def quick_sync_symbol(symbol: str, period: str = "1y") -> bool:
    """Quick utility to sync a single symbol."""
    orchestrator = HistoricalDataOrchestrator(debug=True)
    
    if not orchestrator.initialize():
        return False
    
    result = orchestrator.sync_symbol_historical_data(symbol, period=period)
    orchestrator.close()
    
    return result.success


def sync_top_symbols(count: int = 10, period: str = "1y") -> List[HistoricalSyncResult]:
    """Sync historical data for top N symbols."""
    orchestrator = HistoricalDataOrchestrator()
    
    if not orchestrator.initialize():
        return []
    
    # Get top symbols (first N from the list)
    symbols = load_nasdaq_symbols()[:count]
    results = []
    
    for symbol in symbols:
        result = orchestrator.sync_symbol_historical_data(symbol, period=period)
        results.append(result)
    
    orchestrator.close()
    return results


def create_test_data_sample():
    """Create a small sample of historical data for testing."""
    test_symbols = ['META', 'TESLA', 'GOOGL']
    
    orchestrator = HistoricalDataOrchestrator(debug=True)
    
    if not orchestrator.initialize():
        return False
    
    results = []
    for symbol in test_symbols:
        result = orchestrator.sync_symbol_historical_data(symbol, period="1m")  # 1 month
        results.append(result)
    
    orchestrator.close()
    
    success_count = len([r for r in results if r.success])
    print(f"Test data creation: {success_count}/{len(test_symbols)} symbols successful")
    
    return success_count == len(test_symbols)