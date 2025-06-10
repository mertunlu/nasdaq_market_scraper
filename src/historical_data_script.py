#!/usr/bin/env python3
"""
Script to fetch and store 1 year of historical NASDAQ-100 data using Tiingo API.

This script will:
1. Load NASDAQ-100 symbols
2. Fetch 1 year of historical data from Tiingo
3. Process and validate the data
4. Store in DynamoDB historical table
5. Provide progress reporting and error handling

Usage:
    python historical_data_script.py --mode fetch --symbols all
    python historical_data_script.py --mode update --days 30
    python historical_data_script.py --mode analyze --symbol AAPL
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.config import config
from src.utils import setup_logging, load_nasdaq_symbols, create_nasdaq_symbols_file
from tiingo_historical_fetcher import TiingoHistoricalFetcher, HistoricalDataManager, HistoricalStockData
from historical_database_manager import HistoricalDatabaseManager, HistoricalDataAnalyzer


class HistoricalDataOrchestrator:
    """Orchestrates historical data operations."""
    
    def __init__(self):
        self.logger = setup_logging()
        self.tiingo_fetcher = None
        self.db_manager = None
        self.analyzer = None
        
        # Progress tracking
        self.total_symbols = 0
        self.processed_symbols = 0
        self.successful_symbols = 0
        self.total_records = 0
        
    def initialize(self, api_token: str = None) -> bool:
        """Initialize all components."""
        try:
            self.logger.info("Initializing historical data orchestrator...")
            
            # Initialize Tiingo fetcher
            if not api_token:
                api_token = os.getenv('TIINGO_API_TOKEN')
                if not api_token:
                    self.logger.error("Tiingo API token not found. Set TIINGO_API_TOKEN environment variable.")
                    return False
            
            self.tiingo_fetcher = TiingoHistoricalFetcher(api_token)
            
            # Initialize database manager
            self.db_manager = HistoricalDatabaseManager()
            
            # Create table if needed
            if not self.db_manager.create_historical_table_if_not_exists():
                self.logger.error("Failed to create or access historical database table")
                return False
            
            # Test connections
            if not self.db_manager.test_connection():
                self.logger.error("Historical database connection failed")
                return False
            
            # Initialize analyzer
            self.analyzer = HistoricalDataAnalyzer(self.db_manager)
            
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize orchestrator: {e}")
            return False
    
    def fetch_full_historical_data(self, symbols: List[str] = None, days_back: int = 365) -> Dict[str, Any]:
        """Fetch and store full historical data for symbols."""
        if symbols is None:
            try:
                symbols = load_nasdaq_symbols()
            except Exception as e:
                self.logger.warning(f"Could not load symbols from file: {e}")
                self.logger.info("Creating default symbols file...")
                create_nasdaq_symbols_file()
                symbols = load_nasdaq_symbols()
        
        self.total_symbols = len(symbols)
        self.processed_symbols = 0
        self.successful_symbols = 0
        self.total_records = 0
        
        self.logger.info(f"Starting historical data fetch for {self.total_symbols} symbols")
        self.logger.info(f"Fetching {days_back} days of data")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        failed_symbols = []
        successful_symbols = []
        
        # Process symbols in batches for memory efficiency
        batch_size = 10
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            
            self.logger.info(f"Processing batch {i//batch_size + 1}: symbols {i+1}-{min(i+batch_size, len(symbols))}")
            
            for symbol in batch_symbols:
                try:
                    self._process_single_symbol(symbol, start_date_str, end_date_str)
                    successful_symbols.append(symbol)
                    self.successful_symbols += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {symbol}: {e}")
                    failed_symbols.append(symbol)
                finally:
                    self.processed_symbols += 1
                    self._log_progress()
        
        # Summary
        results = {
            'total_symbols': self.total_symbols,
            'successful_symbols': self.successful_symbols,
            'failed_symbols': len(failed_symbols),
            'total_records_saved': self.total_records,
            'success_rate': (self.successful_symbols / self.total_symbols * 100) if self.total_symbols > 0 else 0,
            'failed_symbol_list': failed_symbols,
            'successful_symbol_list': successful_symbols,
            'date_range': {
                'start_date': start_date_str,
                'end_date': end_date_str
            },
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        self.logger.info(f"Historical data fetch completed:")
        self.logger.info(f"  Success: {self.successful_symbols}/{self.total_symbols} symbols ({results['success_rate']:.1f}%)")
        self.logger.info(f"  Total records: {self.total_records}")
        
        if failed_symbols:
            self.logger.warning(f"  Failed symbols: {failed_symbols}")
        
        return results
    
    def _process_single_symbol(self, symbol: str, start_date: str, end_date: str):
        """Process historical data for a single symbol."""
        self.logger.debug(f"Processing {symbol}...")
        
        # Check if we already have recent data
        latest_date = self.db_manager.get_latest_date(symbol)
        if latest_date:
            self.logger.debug(f"{symbol} has data until {latest_date}")
            # Only fetch data after the latest date we have
            start_date = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            if start_date >= end_date:
                self.logger.debug(f"{symbol} is up to date")
                return
        
        # Fetch historical data from Tiingo
        historical_data = self.tiingo_fetcher.fetch_historical_data(symbol, start_date, end_date)
        
        if not historical_data:
            self.logger.warning(f"No historical data returned for {symbol}")
            return
        
        # Convert to dict format for database
        db_records = []
        for hist_data in historical_data:
            db_records.append(hist_data.to_dict())
        
        # Save to database
        saved_count = self.db_manager.save_historical_data(symbol, db_records)
        self.total_records += saved_count
        
        self.logger.info(f"✓ {symbol}: {saved_count} records saved")
    
    def _log_progress(self):
        """Log progress updates."""
        if self.processed_symbols % 10 == 0 or self.processed_symbols == self.total_symbols:
            progress_percent = (self.processed_symbols / self.total_symbols * 100) if self.total_symbols > 0 else 0
            self.logger.info(
                f"Progress: {self.processed_symbols}/{self.total_symbols} symbols "
                f"({progress_percent:.1f}%) - {self.successful_symbols} successful"
            )
    
    def update_recent_data(self, symbols: List[str] = None, days_back: int = 30) -> Dict[str, Any]:
        """Update recent historical data for symbols."""
        if symbols is None:
            symbols = load_nasdaq_symbols()
        
        self.logger.info(f"Updating recent data for {len(symbols)} symbols ({days_back} days back)")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        updated_symbols = []
        failed_symbols = []
        total_updated_records = 0
        
        for symbol in symbols:
            try:
                # Fetch recent data
                historical_data = self.tiingo_fetcher.fetch_historical_data(symbol, start_date, end_date)
                
                if historical_data:
                    # Convert to dict format
                    db_records = [hist_data.to_dict() for hist_data in historical_data]
                    
                    # Update in database (avoiding duplicates)
                    updated_count = self.db_manager.update_historical_data(symbol, db_records)
                    total_updated_records += updated_count
                    updated_symbols.append(symbol)
                    
                    self.logger.debug(f"✓ {symbol}: {updated_count} records updated")
                else:
                    self.logger.warning(f"No recent data for {symbol}")
                    
            except Exception as e:
                self.logger.error(f"Failed to update {symbol}: {e}")
                failed_symbols.append(symbol)
        
        results = {
            'updated_symbols': len(updated_symbols),
            'failed_symbols': len(failed_symbols),
            'total_updated_records': total_updated_records,
            'date_range': {'start_date': start_date, 'end_date': end_date},
            'failed_symbol_list': failed_symbols,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        self.logger.info(f"Update completed: {len(updated_symbols)} symbols, {total_updated_records} records")
        return results
    
    def analyze_symbol(self, symbol: str, days: int = 30) -> Dict[str, Any]:
        """Analyze historical data for a specific symbol."""
        self.logger.info(f"Analyzing {symbol} over {days} days")
        
        try:
            # Get price statistics
            stats = self.analyzer.get_price_statistics(symbol, days)
            
            if 'error' in stats:
                return stats
            
            # Get additional info
            earliest_date, latest_date = self.db_manager.get_date_range(symbol)
            
            analysis = {
                'symbol': symbol,
                'analysis_period_days': days,
                'price_statistics': stats,
                'data_coverage': {
                    'earliest_date': earliest_date,
                    'latest_date': latest_date,
                    'has_recent_data': self.analyzer._is_recent_date(latest_date) if latest_date else False
                },
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing {symbol}: {e}")
            return {'error': str(e)}
    
    def generate_coverage_report(self) -> Dict[str, Any]:
        """Generate comprehensive data coverage report."""
        self.logger.info("Generating data coverage report...")
        
        try:
            # Get basic coverage report
            coverage_report = self.analyzer.get_data_coverage_report()
            
            # Add summary statistics
            if 'coverage_details' in coverage_report:
                details = coverage_report['coverage_details']
                
                # Calculate summary stats
                total_records = sum(details[symbol]['total_records'] for symbol in details)
                symbols_with_recent_data = sum(1 for symbol in details if details[symbol]['has_recent_data'])
                
                # Find date ranges
                all_earliest_dates = [details[symbol]['earliest_date'] for symbol in details if details[symbol]['earliest_date']]
                all_latest_dates = [details[symbol]['latest_date'] for symbol in details if details[symbol]['latest_date']]
                
                coverage_report['summary'] = {
                    'total_records_across_all_symbols': total_records,
                    'symbols_with_recent_data': symbols_with_recent_data,
                    'overall_earliest_date': min(all_earliest_dates) if all_earliest_dates else None,
                    'overall_latest_date': max(all_latest_dates) if all_latest_dates else None,
                    'data_completeness_percent': (symbols_with_recent_data / len(details) * 100) if details else 0
                }
            
            return coverage_report
            
        except Exception as e:
            self.logger.error(f"Error generating coverage report: {e}")
            return {'error': str(e)}
    
    def cleanup(self):
        """Clean up resources."""
        if self.tiingo_fetcher:
            self.tiingo_fetcher.close()
        self.logger.info("Resources cleaned up")


def main():
    """Main entry point with command line argument handling."""
    parser = argparse.ArgumentParser(description='NASDAQ-100 Historical Data Manager')
    parser.add_argument(
        '--mode', 
        choices=['fetch', 'update', 'analyze', 'report'],
        required=True,
        help='Operation mode'
    )
    parser.add_argument(
        '--symbols',
        nargs='*',
        help='Specific symbols to process (default: all NASDAQ-100)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=365,
        help='Number of days back to fetch/update (default: 365)'
    )
    parser.add_argument(
        '--symbol',
        type=str,
        help='Single symbol for analysis mode'
    )
    parser.add_argument(
        '--api-token',
        type=str,
        help='Tiingo API token (or set TIINGO_API_TOKEN env var)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level'
    )
    
    args = parser.parse_args()
    
    # Set log level
    if args.log_level:
        os.environ['LOG_LEVEL'] = args.log_level
    
    # Initialize orchestrator
    orchestrator = HistoricalDataOrchestrator()
    
    try:
        # Initialize components
        if not orchestrator.initialize(args.api_token):
            print("Failed to initialize historical data orchestrator")
            return 1
        
        # Execute based on mode
        if args.mode == 'fetch':
            symbols = args.symbols if args.symbols and args.symbols != ['all'] else None
            results = orchestrator.fetch_full_historical_data(symbols, args.days)
            print(f"\nFetch Results:")
            print(f"  Success Rate: {results['success_rate']:.1f}%")
            print(f"  Total Records: {results['total_records_saved']}")
            
        elif args.mode == 'update':
            symbols = args.symbols if args.symbols and args.symbols != ['all'] else None
            results = orchestrator.update_recent_data(symbols, args.days)
            print(f"\nUpdate Results:")
            print(f"  Updated Symbols: {results['updated_symbols']}")
            print(f"  Updated Records: {results['total_updated_records']}")
            
        elif args.mode == 'analyze':
            if not args.symbol:
                print("Error: --symbol required for analyze mode")
                return 1
            results = orchestrator.analyze_symbol(args.symbol, args.days)
            if 'error' not in results:
                stats = results['price_statistics']
                print(f"\nAnalysis for {args.symbol}:")
                print(f"  Current Price: ${stats['price_current']:.2f}")
                print(f"  Total Return: {stats['total_return_percent']:.2f}%")
                print(f"  Volatility: {stats['volatility']:.2f}%")
                print(f"  Best Day: {stats['best_day_percent']:.2f}%")
                print(f"  Worst Day: {stats['worst_day_percent']:.2f}%")
            else:
                print(f"Analysis error: {results['error']}")
                
        elif args.mode == 'report':
            results = orchestrator.generate_coverage_report()
            if 'error' not in results:
                print(f"\nData Coverage Report:")
                print(f"  Total Symbols: {results['total_symbols']}")
                if 'summary' in results:
                    summary = results['summary']
                    print(f"  Total Records: {summary['total_records_across_all_symbols']}")
                    print(f"  Symbols with Recent Data: {summary['symbols_with_recent_data']}")
                    print(f"  Data Completeness: {summary['data_completeness_percent']:.1f}%")
                    print(f"  Date Range: {summary['overall_earliest_date']} to {summary['overall_latest_date']}")
            else:
                print(f"Report error: {results['error']}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        orchestrator.cleanup()


if __name__ == '__main__':
    sys.exit(main())