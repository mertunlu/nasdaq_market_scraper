"""Health check functionality for the NASDAQ-100 scraper."""

import logging
import requests
from datetime import datetime
from typing import Dict, Any

from src.config import config
from src.models import HealthStatus
from src.utils import get_system_info
from src.database import DynamoDBManager
from src.exceptions import DatabaseError, NetworkError


class HealthChecker:
    """Performs comprehensive health checks for the scraper system."""
    
    def __init__(self, db_manager: DynamoDBManager = None):
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager or DynamoDBManager()
    
    def check_database_connection(self) -> bool:
        """Check DynamoDB connectivity and accessibility."""
        try:
            return self.db_manager.test_connection()
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False
    
    def check_internet_connection(self) -> bool:
        """Check internet connectivity by testing Yahoo Finance."""
        try:
            response = requests.get(
                "https://finance.yahoo.com",
                timeout=10,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; HealthCheck/1.0)'}
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Internet connectivity check failed: {e}")
            return False
    
    def check_yahoo_finance_access(self) -> Dict[str, Any]:
        """Check specific Yahoo Finance access with a test symbol."""
        try:
            test_url = f"{config.YAHOO_FINANCE_BASE_URL}AAPL"
            response = requests.get(
                test_url,
                timeout=config.REQUEST_TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; HealthCheck/1.0)'}
            )
            
            return {
                'accessible': response.status_code == 200,
                'status_code': response.status_code,
                'response_time_ms': response.elapsed.total_seconds() * 1000,
                'content_length': len(response.content) if response.content else 0
            }
        except Exception as e:
            return {
                'accessible': False,
                'error': str(e),
                'response_time_ms': 0,
                'content_length': 0
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics and health metrics."""
        try:
            return self.db_manager.get_table_stats()
        except Exception as e:
            self.logger.error(f"Failed to get database stats: {e}")
            return {'error': str(e)}
    
    def perform_comprehensive_check(self) -> HealthStatus:
        """Perform all health checks and return comprehensive status."""
        self.logger.info("Performing comprehensive health check")
        
        # Get system information
        system_info = get_system_info()
        
        # Check individual components
        db_connection = self.check_database_connection()
        internet_connection = self.check_internet_connection()
        yahoo_access = self.check_yahoo_finance_access()
        
        # Determine overall health status
        all_checks = [
            db_connection,
            internet_connection,
            yahoo_access.get('accessible', False)
        ]
        
        overall_status = 'healthy' if all(all_checks) else 'unhealthy'
        
        # Create health status object
        health_status = HealthStatus(
            status=overall_status,
            timestamp=datetime.utcnow().isoformat() + 'Z',
            database_connection=db_connection,
            internet_connection=internet_connection,
            memory_usage_mb=system_info.get('memory_usage_mb', 0),
            disk_space_gb=system_info.get('disk_space_gb', 0)
        )
        
        # Log results
        if overall_status == 'healthy':
            self.logger.info("Health check passed - all systems operational")
        else:
            failed_checks = []
            if not db_connection:
                failed_checks.append("database")
            if not internet_connection:
                failed_checks.append("internet")
            if not yahoo_access.get('accessible', False):
                failed_checks.append("yahoo_finance")
            
            self.logger.warning(f"Health check failed - issues with: {', '.join(failed_checks)}")
        
        return health_status
    
    def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status information for monitoring."""
        health_status = self.perform_comprehensive_check()
        system_info = get_system_info()
        db_stats = self.get_database_stats()
        yahoo_access = self.check_yahoo_finance_access()
        
        return {
            'overall_status': health_status.status,
            'timestamp': health_status.timestamp,
            'checks': {
                'database': {
                    'connected': health_status.database_connection,
                    'stats': db_stats
                },
                'internet': {
                    'connected': health_status.internet_connection
                },
                'yahoo_finance': yahoo_access,
                'system': system_info
            },
            'summary': {
                'memory_usage_mb': health_status.memory_usage_mb,
                'disk_space_gb': health_status.disk_space_gb,
                'all_systems_operational': health_status.status == 'healthy'
            }
        }