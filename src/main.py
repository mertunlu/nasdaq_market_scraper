"""Main application entry point for NASDAQ-100 scraper."""

import sys
import time
import signal
import logging
import schedule
from datetime import datetime
from typing import Optional

from src.config import config
from src.database import DynamoDBManager
from src.scraper import NasdaqScraper
from src.health import HealthChecker
from src.utils import setup_logging, ensure_directory_exists
from src.exceptions import DatabaseError, ConfigurationError


class NasdaqScraperApp:
    """Main application class that orchestrates the NASDAQ-100 scraper."""
    
    def __init__(self):
        self.logger = setup_logging()
        self.running = False
        self.db_manager: Optional[DynamoDBManager] = None
        self.scraper: Optional[NasdaqScraper] = None
        self.health_checker: Optional[HealthChecker] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("NASDAQ-100 Scraper Application initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_names = {signal.SIGINT: 'SIGINT', signal.SIGTERM: 'SIGTERM'}
        signal_name = signal_names.get(signum, f'Signal {signum}')
        
        self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        self.stop()
    
    def initialize(self) -> bool:
        """Initialize all components and verify system health."""
        try:
            self.logger.info("Initializing application components...")
            
            # Ensure required directories exist
            ensure_directory_exists(config.LOG_FILE_PATH)
            ensure_directory_exists(config.NASDAQ_SYMBOLS_FILE)
            
            # Initialize database manager
            self.logger.info("Initializing database connection...")
            self.db_manager = DynamoDBManager()
            
            # Test database connection and create table if needed
            if not self.db_manager.test_connection():
                self.logger.info("Database table not found, attempting to create...")
                if not self.db_manager.create_table_if_not_exists():
                    raise DatabaseError("Failed to create or access database table")
            
            # Initialize scraper
            self.logger.info("Initializing NASDAQ scraper...")
            self.scraper = NasdaqScraper(debug=config.DEBUG)
            
            # Initialize health checker
            self.health_checker = HealthChecker(self.db_manager)
            
            # Perform initial health check
            self.logger.info("Performing initial health check...")
            health_status = self.health_checker.perform_comprehensive_check()
            
            if health_status.status != 'healthy':
                self.logger.error("Initial health check failed")
                detailed_status = self.health_checker.get_detailed_status()
                self.logger.error(f"Health check details: {detailed_status}")
                return False
            
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize application: {e}")
            return False
    
    def run_single_scrape(self) -> bool:
        """Execute a single scraping cycle."""
        try:
            start_time = datetime.utcnow()
            self.logger.info("Starting scraping cycle...")
            
            # Perform the scraping
            batch_result = self.scraper.scrape_all()
            
            # Store successful results in database
            successful_data = batch_result.get_successful_data()
            if successful_data:
                self.logger.info(f"Saving {len(successful_data)} stock records to database...")
                success_count, failed_symbols = self.db_manager.save_batch_stock_data(successful_data)
                
                if failed_symbols:
                    self.logger.warning(f"Failed to save {len(failed_symbols)} records: {failed_symbols}")
                
                self.logger.info(f"Successfully saved {success_count} stock records")
            else:
                self.logger.warning("No successful data to save")
            
            # Log scraping statistics
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.logger.info(
                f"Scraping cycle completed in {duration:.1f}s: "
                f"{batch_result.successful}/{batch_result.total_symbols} successful "
                f"({batch_result.success_rate:.1f}%)"
            )
            
            # Log failed symbols if any
            if batch_result.failed > 0:
                failed_symbols = batch_result.get_failed_symbols()
                self.logger.warning(f"Failed symbols: {failed_symbols}")
            
            return batch_result.success_rate >= 80  # Consider success if >80% scraped successfully
            
        except Exception as e:
            self.logger.error(f"Error during scraping cycle: {e}")
            return False
    
    def run_health_check(self):
        """Perform periodic health check."""
        try:
            self.logger.debug("Performing scheduled health check...")
            health_status = self.health_checker.perform_comprehensive_check()
            
            if health_status.status != 'healthy':
                self.logger.warning("Health check indicates system issues")
                detailed_status = self.health_checker.get_detailed_status()
                self.logger.warning(f"Health details: {detailed_status}")
            else:
                self.logger.debug("Health check passed")
                
        except Exception as e:
            self.logger.error(f"Error during health check: {e}")
    
    def schedule_jobs(self):
        """Schedule recurring jobs."""
        # Schedule scraping every minute (or based on config)
        schedule.every(config.SCRAPE_INTERVAL).seconds.do(self.run_single_scrape)
        
        # Schedule health checks every 5 minutes
        schedule.every(config.HEALTH_CHECK_INTERVAL).seconds.do(self.run_health_check)
        
        # Schedule statistics logging every hour
        schedule.every().hour.do(self._log_statistics)
        
        self.logger.info(
            f"Scheduled jobs: scraping every {config.SCRAPE_INTERVAL}s, "
            f"health checks every {config.HEALTH_CHECK_INTERVAL}s"
        )
    
    def _log_statistics(self):
        """Log system statistics periodically."""
        try:
            # Get scraper stats
            scraper_stats = self.scraper.get_stats()
            
            # Get database stats
            db_stats = self.db_manager.get_table_stats()
            
            self.logger.info(
                f"Hourly Statistics - "
                f"Requests: {scraper_stats['requests_made']}, "
                f"Success Rate: {scraper_stats['success_rate_percent']:.1f}%, "
                f"DB Records: {db_stats.get('item_count', 0)}"
            )
            
        except Exception as e:
            self.logger.error(f"Error logging statistics: {e}")
    
    def run_daemon(self):
        """Run the application as a daemon with scheduled jobs."""
        if not self.initialize():
            self.logger.error("Failed to initialize application")
            return 1
        
        self.running = True
        self.schedule_jobs()
        
        # Run initial scrape immediately
        self.logger.info("Running initial scraping cycle...")
        self.run_single_scrape()
        
        self.logger.info("Starting daemon mode - press Ctrl+C to stop")
        
        # Main event loop
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(1)  # Check every second
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.stop()
        
        return 0
    
    def run_once(self):
        """Run a single scraping cycle and exit."""
        if not self.initialize():
            self.logger.error("Failed to initialize application")
            return 1
        
        self.logger.info("Running single scraping cycle...")
        success = self.run_single_scrape()
        
        if success:
            self.logger.info("Single scrape completed successfully")
            return 0
        else:
            self.logger.error("Single scrape failed")
            return 1
    
    def run_health_check_only(self):
        """Run health check only and exit."""
        if not self.initialize():
            self.logger.error("Failed to initialize application")
            return 1
        
        self.logger.info("Running health check...")
        detailed_status = self.health_checker.get_detailed_status()
        
        print("\n=== NASDAQ Scraper Health Check ===")
        print(f"Overall Status: {detailed_status['overall_status'].upper()}")
        print(f"Timestamp: {detailed_status['timestamp']}")
        print("\nComponent Status:")
        print(f"  Database: {'✓' if detailed_status['checks']['database']['connected'] else '✗'}")
        print(f"  Internet: {'✓' if detailed_status['checks']['internet']['connected'] else '✗'}")
        print(f"  Yahoo Finance: {'✓' if detailed_status['checks']['yahoo_finance']['accessible'] else '✗'}")
        print(f"\nSystem Resources:")
        print(f"  Memory Usage: {detailed_status['summary']['memory_usage_mb']:.1f} MB")
        print(f"  Disk Space: {detailed_status['summary']['disk_space_gb']:.1f} GB")
        
        if detailed_status['checks']['database']['connected']:
            db_stats = detailed_status['checks']['database']['stats']
            print(f"\nDatabase Info:")
            print(f"  Table: {db_stats.get('table_name', 'N/A')}")
            print(f"  Status: {db_stats.get('table_status', 'N/A')}")
            print(f"  Records: {db_stats.get('item_count', 0)}")
        
        return 0 if detailed_status['overall_status'] == 'healthy' else 1
    
    def stop(self):
        """Stop the application gracefully."""
        self.logger.info("Stopping application...")
        self.running = False
        
        # Clean up resources
        if self.scraper:
            self.scraper.close()
        
        # Clear scheduled jobs
        schedule.clear()
        
        self.logger.info("Application stopped successfully")


def main():
    """Main entry point with command line argument handling."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NASDAQ-100 Stock Data Scraper')
    parser.add_argument(
        '--mode', 
        choices=['daemon', 'once', 'health'],
        default='daemon',
        help='Run mode: daemon (continuous), once (single run), health (health check only)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with reduced symbol set'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=None,
        help='Set logging level'
    )
    
    args = parser.parse_args()
    
    # Override config with command line arguments
    if args.debug:
        import os
        os.environ['DEBUG'] = 'true'
    
    if args.log_level:
        import os
        os.environ['LOG_LEVEL'] = args.log_level
    
    # Create and run application
    app = NasdaqScraperApp()
    
    try:
        if args.mode == 'daemon':
            return app.run_daemon()
        elif args.mode == 'once':
            return app.run_once()
        elif args.mode == 'health':
            return app.run_health_check_only()
    except Exception as e:
        logging.error(f"Application failed with error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())