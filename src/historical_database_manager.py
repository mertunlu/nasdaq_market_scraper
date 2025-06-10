"""Historical database operations for NASDAQ-100 scraper."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import boto3
from boto3.dynamodb.conditions import Key, Attr

from src.config import config
from src.exceptions import DatabaseError, AuthenticationError
from src.utils import retry_with_backoff, chunk_list


class HistoricalDatabaseManager:
    """Manages DynamoDB operations for historical stock data."""
    
    def __init__(self, table_name: str = None, region: str = None):
        self.table_name = table_name or f"{config.DYNAMODB_TABLE_NAME}_historical"
        self.region = region or config.AWS_REGION
        self.logger = logging.getLogger(__name__)
        
        try:
            # Initialize DynamoDB resources
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.client = boto3.client('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)
            
            self.logger.info(f"Historical DynamoDB manager initialized for table: {self.table_name}")
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AuthenticationError(f"AWS credentials not found or incomplete: {e}")
        except Exception as e:
            raise DatabaseError(f"Failed to initialize historical DynamoDB: {e}")
    
    def create_historical_table_if_not_exists(self) -> bool:
        """Create the historical stocks table if it doesn't exist."""
        try:
            # Check if table exists
            self.table.meta.client.describe_table(TableName=self.table_name)
            self.logger.info(f"Historical table {self.table_name} already exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise DatabaseError(f"Error checking historical table existence: {e}")
        
        try:
            # Create table with composite key: symbol (partition) + date (sort)
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'symbol',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'date',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'symbol',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'date',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'  # On-demand billing
            )
            
            # Wait for table to be created
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.logger.info(f"Successfully created historical table: {self.table_name}")
            return True
            
        except ClientError as e:
            raise DatabaseError(f"Failed to create historical table: {e}")
    
    def test_connection(self) -> bool:
        """Test DynamoDB connection and table access."""
        try:
            response = self.table.meta.client.describe_table(TableName=self.table_name)
            table_status = response['Table']['TableStatus']
            
            if table_status == 'ACTIVE':
                self.logger.info(f"Historical DynamoDB connection successful. Table status: {table_status}")
                return True
            else:
                self.logger.warning(f"Historical table exists but status is: {table_status}")
                return False
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'ResourceNotFoundException':
                self.logger.error(f"Historical table {self.table_name} does not exist")
                return False
            elif error_code == 'AccessDeniedException':
                self.logger.error("Access denied to historical DynamoDB table")
                return False
            else:
                self.logger.error(f"Historical DynamoDB connection test failed: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Unexpected error testing historical DynamoDB connection: {e}")
            return False
    
    @retry_with_backoff(max_retries=3)
    def save_historical_record(self, historical_data: dict) -> bool:
        """Save or update single historical stock record in DynamoDB."""
        try:
            # Convert to DynamoDB item
            item = self._historical_data_to_item(historical_data)
            
            # Put item in table
            response = self.table.put_item(Item=item)
            
            # Check response
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.debug(f"Successfully saved historical data for {historical_data['symbol']} on {historical_data['date']}")
                return True
            else:
                self.logger.warning(f"Unexpected response saving historical data: {response}")
                return False
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"DynamoDB error saving historical data: {error_code} - {e}")
            raise DatabaseError(f"Failed to save historical data: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error saving historical data: {e}")
            raise DatabaseError(f"Unexpected error saving historical data: {e}")
    
    def save_historical_data(self, symbol: str, historical_data_list: List[dict]) -> int:
        """Save multiple historical records for a symbol using batch write."""
        if not historical_data_list:
            return 0
        
        successful_count = 0
        
        # Split into chunks of 25 (DynamoDB batch write limit)
        chunks = chunk_list(historical_data_list, 25)
        
        for chunk in chunks:
            try:
                with self.table.batch_writer() as batch:
                    for historical_data in chunk:
                        try:
                            item = self._historical_data_to_item(historical_data)
                            batch.put_item(Item=item)
                            successful_count += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error preparing historical batch item for {symbol}: {e}")
                
                self.logger.debug(f"Batch wrote {len(chunk)} records for {symbol}")
                
            except ClientError as e:
                self.logger.error(f"Historical batch write failed for {symbol}: {e}")
                # Try individual saves for this chunk
                for historical_data in chunk:
                    try:
                        if self.save_historical_record(historical_data):
                            successful_count += 1
                    except DatabaseError:
                        continue
        
        self.logger.info(f"Saved {successful_count}/{len(historical_data_list)} historical records for {symbol}")
        return successful_count
    
    def get_historical_data(self, symbol: str, start_date: str = None, end_date: str = None) -> List[dict]:
        """Retrieve historical data for a symbol within date range."""
        try:
            # Build query
            key_condition = Key('symbol').eq(symbol.upper())
            
            if start_date and end_date:
                key_condition = key_condition & Key('date').between(start_date, end_date)
            elif start_date:
                key_condition = key_condition & Key('date').gte(start_date)
            elif end_date:
                key_condition = key_condition & Key('date').lte(end_date)
            
            # Query the table
            response = self.table.query(KeyConditionExpression=key_condition)
            
            historical_data = []
            for item in response['Items']:
                historical_data.append(self._item_to_historical_data(item))
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression=key_condition,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response['Items']:
                    historical_data.append(self._item_to_historical_data(item))
            
            # Sort by date
            historical_data.sort(key=lambda x: x['date'])
            
            self.logger.info(f"Retrieved {len(historical_data)} historical records for {symbol}")
            return historical_data
            
        except ClientError as e:
            self.logger.error(f"Error retrieving historical data for {symbol}: {e}")
            raise DatabaseError(f"Failed to retrieve historical data for {symbol}: {e}")
    
    def get_latest_date(self, symbol: str) -> Optional[str]:
        """Get the latest date for which we have historical data for a symbol."""
        try:
            response = self.table.query(
                KeyConditionExpression=Key('symbol').eq(symbol.upper()),
                ScanIndexForward=False,  # Sort in descending order
                Limit=1  # Only get the latest record
            )
            
            if response['Items']:
                return response['Items'][0]['date']
            else:
                return None
                
        except ClientError as e:
            self.logger.error(f"Error getting latest date for {symbol}: {e}")
            return None
    
    def get_date_range(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Get the date range (earliest, latest) for which we have data for a symbol."""
        try:
            # Get earliest date
            earliest_response = self.table.query(
                KeyConditionExpression=Key('symbol').eq(symbol.upper()),
                ScanIndexForward=True,  # Sort in ascending order
                Limit=1
            )
            
            # Get latest date
            latest_response = self.table.query(
                KeyConditionExpression=Key('symbol').eq(symbol.upper()),
                ScanIndexForward=False,  # Sort in descending order
                Limit=1
            )
            
            earliest_date = earliest_response['Items'][0]['date'] if earliest_response['Items'] else None
            latest_date = latest_response['Items'][0]['date'] if latest_response['Items'] else None
            
            return earliest_date, latest_date
            
        except ClientError as e:
            self.logger.error(f"Error getting date range for {symbol}: {e}")
            return None, None
    
    def delete_historical_data(self, symbol: str, date: str = None) -> bool:
        """Delete historical data for a symbol on a specific date or all dates."""
        try:
            if date:
                # Delete specific date
                response = self.table.delete_item(
                    Key={'symbol': symbol.upper(), 'date': date}
                )
                success = response['ResponseMetadata']['HTTPStatusCode'] == 200
                if success:
                    self.logger.info(f"Deleted historical data for {symbol} on {date}")
                return success
            else:
                # Delete all historical data for symbol
                # First, get all dates for this symbol
                historical_data = self.get_historical_data(symbol)
                
                deleted_count = 0
                for data in historical_data:
                    try:
                        self.table.delete_item(
                            Key={'symbol': symbol.upper(), 'date': data['date']}
                        )
                        deleted_count += 1
                    except Exception as e:
                        self.logger.error(f"Error deleting {symbol} on {data['date']}: {e}")
                
                self.logger.info(f"Deleted {deleted_count} historical records for {symbol}")
                return deleted_count > 0
                
        except ClientError as e:
            self.logger.error(f"Error deleting historical data for {symbol}: {e}")
            raise DatabaseError(f"Failed to delete historical data for {symbol}: {e}")
    
    def get_symbols_with_data(self) -> List[str]:
        """Get list of all symbols that have historical data."""
        try:
            symbols = set()
            
            # Scan to get all unique symbols
            response = self.table.scan(
                ProjectionExpression='symbol'
            )
            
            for item in response['Items']:
                symbols.add(item['symbol'])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    ProjectionExpression='symbol',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response['Items']:
                    symbols.add(item['symbol'])
            
            symbol_list = sorted(list(symbols))
            self.logger.info(f"Found historical data for {len(symbol_list)} symbols")
            return symbol_list
            
        except ClientError as e:
            self.logger.error(f"Error getting symbols with historical data: {e}")
            raise DatabaseError(f"Failed to get symbols with historical data: {e}")
    
    def get_table_stats(self) -> Dict[str, Any]:
        """Get historical table statistics and information."""
        try:
            # Get table description
            response = self.table.meta.client.describe_table(TableName=self.table_name)
            table_info = response['Table']
            
            # Count items (scan with Select='COUNT')
            count_response = self.table.scan(Select='COUNT')
            item_count = count_response['Count']
            
            # Get additional scanned count if pagination occurred
            while 'LastEvaluatedKey' in count_response:
                count_response = self.table.scan(
                    Select='COUNT',
                    ExclusiveStartKey=count_response['LastEvaluatedKey']
                )
                item_count += count_response['Count']
            
            # Get symbols count
            symbols = self.get_symbols_with_data()
            
            stats = {
                'table_name': self.table_name,
                'table_status': table_info['TableStatus'],
                'total_records': item_count,
                'unique_symbols': len(symbols),
                'table_size_bytes': table_info.get('TableSizeBytes', 0),
                'creation_datetime': table_info.get('CreationDateTime'),
                'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode', 'Unknown'),
                'last_updated': datetime.utcnow().isoformat() + 'Z'
            }
            
            return stats
            
        except ClientError as e:
            self.logger.error(f"Error getting historical table stats: {e}")
            raise DatabaseError(f"Failed to get historical table statistics: {e}")
    
    def update_historical_data(self, symbol: str, new_data_list: List[dict]) -> int:
        """Update historical data for a symbol, avoiding duplicates."""
        if not new_data_list:
            return 0
        
        updated_count = 0
        
        # Check which dates already exist
        existing_dates = set()
        try:
            existing_data = self.get_historical_data(symbol)
            existing_dates = {data['date'] for data in existing_data}
        except Exception as e:
            self.logger.warning(f"Could not check existing dates for {symbol}: {e}")
        
        # Filter out existing dates
        new_records = []
        for data in new_data_list:
            if data.get('date') not in existing_dates:
                new_records.append(data)
            else:
                # Update existing record
                try:
                    if self.save_historical_record(data):
                        updated_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to update existing record for {symbol} on {data.get('date')}: {e}")
        
        # Save new records
        if new_records:
            saved_count = self.save_historical_data(symbol, new_records)
            updated_count += saved_count
        
        self.logger.info(f"Updated {updated_count} records for {symbol}")
        return updated_count
    
    def get_missing_dates(self, symbol: str, start_date: str, end_date: str) -> List[str]:
        """Get list of dates missing historical data for a symbol in given range."""
        try:
            from datetime import datetime, timedelta
            
            # Get existing data
            existing_data = self.get_historical_data(symbol, start_date, end_date)
            existing_dates = {data['date'] for data in existing_data}
            
            # Generate all dates in range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_dates = []
            current_dt = start_dt
            while current_dt <= end_dt:
                # Skip weekends (assuming stock market is closed)
                if current_dt.weekday() < 5:  # Monday = 0, Friday = 4
                    all_dates.append(current_dt.strftime('%Y-%m-%d'))
                current_dt += timedelta(days=1)
            
            # Find missing dates
            missing_dates = [date for date in all_dates if date not in existing_dates]
            
            self.logger.info(f"Found {len(missing_dates)} missing dates for {symbol}")
            return missing_dates
            
        except Exception as e:
            self.logger.error(f"Error finding missing dates for {symbol}: {e}")
            return []
    
    def _historical_data_to_item(self, historical_data: dict) -> Dict[str, Any]:
        """Convert historical data dictionary to DynamoDB item."""
        return {
            'symbol': historical_data['symbol'],
            'date': historical_data['date'],
            'open': historical_data['open'],
            'high': historical_data['high'],
            'low': historical_data['low'],
            'close': historical_data['close'],
            'volume': historical_data['volume'],
            'daily_change_nominal': historical_data['daily_change_nominal'],
            'daily_change_percent': historical_data['daily_change_percent'],
            'previous_close': historical_data['previous_close'],
            'market': historical_data.get('market', 'NASDAQ')
        }
    
    def _item_to_historical_data(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB item to historical data dictionary."""
        return {
            'symbol': item['symbol'],
            'date': item['date'],
            'open': Decimal(str(item['open'])),
            'high': Decimal(str(item['high'])),
            'low': Decimal(str(item['low'])),
            'close': Decimal(str(item['close'])),
            'volume': int(item['volume']),
            'daily_change_nominal': Decimal(str(item['daily_change_nominal'])),
            'daily_change_percent': Decimal(str(item['daily_change_percent'])),
            'previous_close': Decimal(str(item['previous_close'])),
            'market': item.get('market', 'NASDAQ')
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform historical database health check."""
        try:
            # Test basic connectivity
            connection_ok = self.test_connection()
            
            # Get basic stats
            stats = self.get_table_stats() if connection_ok else {}
            
            # Test a simple operation
            test_symbol = 'AAPL'
            test_read_ok = False
            try:
                self.get_latest_date(test_symbol)
                test_read_ok = True
            except Exception as e:
                self.logger.debug(f"Historical test read failed: {e}")
            
            return {
                'historical_database_connection': connection_ok,
                'historical_table_accessible': connection_ok,
                'historical_test_read_successful': test_read_ok,
                'historical_total_records': stats.get('total_records', 0),
                'historical_unique_symbols': stats.get('unique_symbols', 0),
                'historical_table_status': stats.get('table_status', 'Unknown'),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
        except Exception as e:
            self.logger.error(f"Historical database health check failed: {e}")
            return {
                'historical_database_connection': False,
                'historical_table_accessible': False,
                'historical_test_read_successful': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }


class HistoricalDataAnalyzer:
    """Provides analysis capabilities for historical stock data."""
    
    def __init__(self, db_manager: HistoricalDatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    def get_price_statistics(self, symbol: str, days: int = 30) -> Dict[str, Any]:
        """Get price statistics for a symbol over specified days."""
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        try:
            historical_data = self.db_manager.get_historical_data(symbol, start_date, end_date)
            
            if not historical_data:
                return {'error': f'No data found for {symbol}'}
            
            # Calculate statistics
            closes = [float(data['close']) for data in historical_data]
            volumes = [data['volume'] for data in historical_data]
            changes = [float(data['daily_change_percent']) for data in historical_data]
            
            stats = {
                'symbol': symbol,
                'period_days': len(historical_data),
                'price_current': closes[-1] if closes else 0,
                'price_high': max(closes) if closes else 0,
                'price_low': min(closes) if closes else 0,
                'price_average': sum(closes) / len(closes) if closes else 0,
                'volume_average': sum(volumes) / len(volumes) if volumes else 0,
                'volatility': self._calculate_volatility(changes),
                'total_return_percent': ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) > 1 else 0,
                'best_day_percent': max(changes) if changes else 0,
                'worst_day_percent': min(changes) if changes else 0,
                'up_days': len([c for c in changes if c > 0]),
                'down_days': len([c for c in changes if c < 0]),
                'start_date': start_date,
                'end_date': end_date
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics for {symbol}: {e}")
            return {'error': str(e)}
    
    def _calculate_volatility(self, changes: List[float]) -> float:
        """Calculate volatility (standard deviation of daily changes)."""
        if len(changes) < 2:
            return 0.0
        
        mean_change = sum(changes) / len(changes)
        variance = sum((x - mean_change) ** 2 for x in changes) / (len(changes) - 1)
        return variance ** 0.5
    
    def compare_symbols(self, symbols: List[str], days: int = 30) -> Dict[str, Any]:
        """Compare multiple symbols over specified period."""
        comparison = {}
        
        for symbol in symbols:
            stats = self.get_price_statistics(symbol, days)
            if 'error' not in stats:
                comparison[symbol] = {
                    'current_price': stats['price_current'],
                    'total_return_percent': stats['total_return_percent'],
                    'volatility': stats['volatility'],
                    'volume_average': stats['volume_average']
                }
        
        return {
            'comparison': comparison,
            'period_days': days,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    
    def get_data_coverage_report(self) -> Dict[str, Any]:
        """Generate a report on data coverage for all symbols."""
        try:
            symbols = self.db_manager.get_symbols_with_data()
            coverage_report = {}
            
            for symbol in symbols:
                earliest_date, latest_date = self.db_manager.get_date_range(symbol)
                if earliest_date and latest_date:
                    # Calculate number of records
                    historical_data = self.db_manager.get_historical_data(symbol)
                    
                    coverage_report[symbol] = {
                        'earliest_date': earliest_date,
                        'latest_date': latest_date,
                        'total_records': len(historical_data),
                        'has_recent_data': self._is_recent_date(latest_date)
                    }
            
            return {
                'total_symbols': len(symbols),
                'coverage_details': coverage_report,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
        except Exception as e:
            self.logger.error(f"Error generating coverage report: {e}")
            return {'error': str(e)}
    
    def _is_recent_date(self, date_str: str, days_threshold: int = 7) -> bool:
        """Check if a date is within the recent threshold."""
        from datetime import datetime, timedelta
        
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            threshold_date = datetime.now() - timedelta(days=days_threshold)
            return date_obj >= threshold_date
        except Exception:
            return False


# Example usage and testing
if __name__ == "__main__":
    import os
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize historical database manager
        hist_db = HistoricalDatabaseManager()
        
        # Create table if needed
        if hist_db.create_historical_table_if_not_exists():
            print("✓ Historical table created/verified")
        
        # Test connection
        if hist_db.test_connection():
            print("✓ Historical database connection successful!")
            
            # Get table stats
            stats = hist_db.get_table_stats()
            print(f"✓ Table stats: {stats['table_name']} - {stats['total_records']} records")
            
            # Initialize analyzer
            analyzer = HistoricalDataAnalyzer(hist_db)
            
            # Get data coverage report
            coverage = analyzer.get_data_coverage_report()
            if 'error' not in coverage:
                print(f"✓ Data coverage: {coverage['total_symbols']} symbols with data")
            else:
                print(f"⚠ Coverage report error: {coverage['error']}")
        else:
            print("✗ Historical database connection failed")
        
    except Exception as e:
        print(f"✗ Error: {e}")