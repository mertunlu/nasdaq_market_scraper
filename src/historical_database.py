"""DynamoDB operations for historical NASDAQ stock data."""

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Key

from src.config import config
from src.exceptions import DatabaseError
from src.utils import retry_with_backoff


class HistoricalDataManager:
    """Manages DynamoDB operations for historical stock data."""
    
    def __init__(self, table_name: str = None, region: str = None):
        self.table_name = table_name or 'HistoricalNasdaqPrices'
        self.region = region or config.AWS_REGION
        self.logger = logging.getLogger(__name__)
        
        try:
            # Initialize DynamoDB resources
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.client = boto3.client('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)
            
            self.logger.info(f"Historical data manager initialized for table: {self.table_name}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize DynamoDB for historical data: {e}")
    
    def create_table_if_not_exists(self) -> bool:
        """Create the historical prices table if it doesn't exist."""
        try:
            # Check if table exists
            self.table.meta.client.describe_table(TableName=self.table_name)
            self.logger.info(f"Historical table {self.table_name} already exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise DatabaseError(f"Error checking historical table existence: {e}")
        
        try:
            # Create table with composite key (symbol + date)
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
                BillingMode='PAY_PER_REQUEST',  # On-demand billing
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'date-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'date',
                                'KeyType': 'HASH'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        }
                    }
                ]
            )
            
            # Wait for table to be created
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.logger.info(f"Successfully created historical table: {self.table_name}")
            return True
            
        except ClientError as e:
            raise DatabaseError(f"Failed to create historical table: {e}")
    
    @retry_with_backoff(max_retries=3)
    def save_historical_data_point(self, data_point) -> bool:
        """Save a single historical data point."""
        try:
            item = {
                'symbol': data_point.symbol,
                'date': data_point.date,
                'open': data_point.open,
                'high': data_point.high,
                'low': data_point.low,
                'close': data_point.close,
                'adj_close': data_point.adj_close,
                'volume': data_point.volume,
                'daily_change_percent': data_point.daily_change_percent,
                'daily_change_nominal': data_point.daily_change_nominal,
                'created_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            response = self.table.put_item(Item=item)
            
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.debug(f"Saved historical data for {data_point.symbol} on {data_point.date}")
                return True
            else:
                self.logger.warning(f"Unexpected response saving {data_point.symbol}: {response}")
                return False
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"DynamoDB error saving {data_point.symbol} {data_point.date}: {error_code}")
            raise DatabaseError(f"Failed to save historical data: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error saving historical data: {e}")
            raise DatabaseError(f"Unexpected error: {e}")
    
    def save_batch_historical_data(self, data_points: List) -> Tuple[int, List[str]]:
        """Save multiple historical data points using batch write."""
        if not data_points:
            return 0, []
        
        successful_count = 0
        failed_items = []
        
        try:
            # Group by chunks of 25 (DynamoDB batch limit)
            chunk_size = 25
            chunks = [data_points[i:i + chunk_size] for i in range(0, len(data_points), chunk_size)]
            
            for chunk in chunks:
                with self.table.batch_writer() as batch:
                    for data_point in chunk:
                        try:
                            item = {
                                'symbol': data_point.symbol,
                                'date': data_point.date,
                                'open': data_point.open,
                                'high': data_point.high,
                                'low': data_point.low,
                                'close': data_point.close,
                                'adj_close': data_point.adj_close,
                                'volume': data_point.volume,
                                'daily_change_percent': data_point.daily_change_percent,
                                'daily_change_nominal': data_point.daily_change_nominal,
                                'created_at': datetime.utcnow().isoformat() + 'Z'
                            }
                            
                            batch.put_item(Item=item)
                            successful_count += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error preparing batch item for {data_point.symbol} {data_point.date}: {e}")
                            failed_items.append(f"{data_point.symbol}:{data_point.date}")
            
            self.logger.info(f"Batch historical data write completed: {successful_count} successful, {len(failed_items)} failed")
            
        except ClientError as e:
            self.logger.error(f"Batch write failed: {e}")
            # Fallback to individual saves
            return self._fallback_individual_saves(data_points)
        
        return successful_count, failed_items
    
    def _fallback_individual_saves(self, data_points: List) -> Tuple[int, List[str]]:
        """Fallback to individual saves if batch write fails."""
        self.logger.info("Falling back to individual saves for historical data")
        successful_count = 0
        failed_items = []
        
        for data_point in data_points:
            try:
                if self.save_historical_data_point(data_point):
                    successful_count += 1
                else:
                    failed_items.append(f"{data_point.symbol}:{data_point.date}")
            except DatabaseError:
                failed_items.append(f"{data_point.symbol}:{data_point.date}")
        
        return successful_count, failed_items
    
    def get_symbol_historical_data(
        self, 
        symbol: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve historical data for a specific symbol."""
        try:
            # Build query parameters
            key_condition = Key('symbol').eq(symbol.upper())
            
            if start_date and end_date:
                key_condition = key_condition & Key('date').between(start_date, end_date)
            elif start_date:
                key_condition = key_condition & Key('date').gte(start_date)
            elif end_date:
                key_condition = key_condition & Key('date').lte(end_date)
            
            # Execute query
            query_params = {
                'KeyConditionExpression': key_condition,
                'ScanIndexForward': False  # Sort descending (most recent first)
            }
            
            if limit:
                query_params['Limit'] = limit
            
            response = self.table.query(**query_params)
            items = response['Items']
            
            # Handle pagination if no limit specified
            while 'LastEvaluatedKey' in response and not limit:
                response = self.table.query(
                    **query_params,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
            
            self.logger.info(f"Retrieved {len(items)} historical records for {symbol}")
            return items
            
        except ClientError as e:
            self.logger.error(f"Error retrieving historical data for {symbol}: {e}")
            raise DatabaseError(f"Failed to retrieve historical data: {e}")
    
    def get_date_range_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get all symbols' data for a specific date range using GSI."""
        try:
            all_items = []
            
            # Query using the date index
            for single_date in self._date_range(start_date, end_date):
                response = self.client.query(
                    TableName=self.table_name,
                    IndexName='date-index',
                    KeyConditionExpression='#date = :date',
                    ExpressionAttributeNames={
                        '#date': 'date'
                    },
                    ExpressionAttributeValues={
                        ':date': {'S': single_date}
                    }
                )
                
                # Convert DynamoDB format to regular dict
                for item in response['Items']:
                    all_items.append(self._convert_dynamodb_item(item))
            
            self.logger.info(f"Retrieved {len(all_items)} records for date range {start_date} to {end_date}")
            return all_items
            
        except ClientError as e:
            self.logger.error(f"Error retrieving date range data: {e}")
            raise DatabaseError(f"Failed to retrieve date range data: {e}")
    
    def get_latest_date_for_symbol(self, symbol: str) -> Optional[str]:
        """Get the latest date available for a symbol."""
        try:
            response = self.table.query(
                KeyConditionExpression=Key('symbol').eq(symbol.upper()),
                ScanIndexForward=False,  # Descending order
                Limit=1
            )
            
            if response['Items']:
                return response['Items'][0]['date']
            else:
                return None
                
        except ClientError as e:
            self.logger.error(f"Error getting latest date for {symbol}: {e}")
            return None
    
    def check_data_exists(self, symbol: str, date: str) -> bool:
        """Check if data exists for a specific symbol and date."""
        try:
            response = self.table.get_item(
                Key={
                    'symbol': symbol.upper(),
                    'date': date
                }
            )
            return 'Item' in response
            
        except ClientError as e:
            self.logger.error(f"Error checking data existence for {symbol} {date}: {e}")
            return False
    
    def delete_symbol_data(self, symbol: str, date_range: Optional[Tuple[str, str]] = None) -> int:
        """Delete historical data for a symbol (optionally within date range)."""
        try:
            # First, query to get items to delete
            if date_range:
                start_date, end_date = date_range
                key_condition = Key('symbol').eq(symbol.upper()) & Key('date').between(start_date, end_date)
            else:
                key_condition = Key('symbol').eq(symbol.upper())
            
            response = self.table.query(
                KeyConditionExpression=key_condition,
                ProjectionExpression='symbol, #date',
                ExpressionAttributeNames={'#date': 'date'}
            )
            
            items_to_delete = response['Items']
            
            # Delete items in batches
            deleted_count = 0
            chunk_size = 25
            
            for i in range(0, len(items_to_delete), chunk_size):
                chunk = items_to_delete[i:i + chunk_size]
                
                with self.table.batch_writer() as batch:
                    for item in chunk:
                        batch.delete_item(
                            Key={
                                'symbol': item['symbol'],
                                'date': item['date']
                            }
                        )
                        deleted_count += 1
            
            self.logger.info(f"Deleted {deleted_count} historical records for {symbol}")
            return deleted_count
            
        except ClientError as e:
            self.logger.error(f"Error deleting data for {symbol}: {e}")
            raise DatabaseError(f"Failed to delete historical data: {e}")
    
    def get_table_stats(self) -> Dict[str, Any]:
        """Get historical table statistics."""
        try:
            # Get table description
            response = self.table.meta.client.describe_table(TableName=self.table_name)
            table_info = response['Table']
            
            # Count items using scan
            count_response = self.table.scan(Select='COUNT')
            item_count = count_response['Count']
            
            # Handle pagination for count
            while 'LastEvaluatedKey' in count_response:
                count_response = self.table.scan(
                    Select='COUNT',
                    ExclusiveStartKey=count_response['LastEvaluatedKey']
                )
                item_count += count_response['Count']
            
            # Get date range
            date_range = self._get_date_range_stats()
            
            stats = {
                'table_name': self.table_name,
                'table_status': table_info['TableStatus'],
                'item_count': item_count,
                'table_size_bytes': table_info.get('TableSizeBytes', 0),
                'creation_datetime': table_info.get('CreationDateTime'),
                'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode', 'Unknown'),
                'earliest_date': date_range.get('earliest'),
                'latest_date': date_range.get('latest'),
                'last_updated': datetime.utcnow().isoformat() + 'Z'
            }
            
            return stats
            
        except ClientError as e:
            self.logger.error(f"Error getting historical table stats: {e}")
            raise DatabaseError(f"Failed to get table statistics: {e}")
    
    def _get_date_range_stats(self) -> Dict[str, Optional[str]]:
        """Get the earliest and latest dates in the table."""
        try:
            # Get earliest date using GSI
            earliest_response = self.client.query(
                TableName=self.table_name,
                IndexName='date-index',
                ScanIndexForward=True,  # Ascending
                Limit=1,
                ProjectionExpression='#date',
                ExpressionAttributeNames={'#date': 'date'},
                KeyConditionExpression='#date = :date',
                ExpressionAttributeValues={':date': {'S': '1900-01-01'}}  # Dummy query
            )
            
            # Actually, let's use scan for this since GSI query needs exact key
            scan_response = self.table.scan(
                ProjectionExpression='#date',
                ExpressionAttributeNames={'#date': 'date'},
                Limit=1000  # Sample to find min/max
            )
            
            dates = [item['date'] for item in scan_response['Items']]
            
            if dates:
                return {
                    'earliest': min(dates),
                    'latest': max(dates)
                }
            else:
                return {'earliest': None, 'latest': None}
                
        except Exception as e:
            self.logger.warning(f"Could not get date range stats: {e}")
            return {'earliest': None, 'latest': None}
    
    def _date_range(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end date."""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
    
    def _convert_dynamodb_item(self, dynamodb_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB response format to regular dictionary."""
        item = {}
        for key, value in dynamodb_item.items():
            if 'S' in value:  # String
                item[key] = value['S']
            elif 'N' in value:  # Number
                item[key] = Decimal(value['N'])
            elif 'B' in value:  # Boolean
                item[key] = value['B']
            # Add more type conversions as needed
        return item
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on historical data table."""
        try:
            # Test basic connectivity
            table_info = self.table.meta.client.describe_table(TableName=self.table_name)
            table_status = table_info['Table']['TableStatus']
            
            # Test basic operations
            test_query_successful = False
            try:
                self.table.query(
                    KeyConditionExpression=Key('symbol').eq('TEST'),
                    Limit=1
                )
                test_query_successful = True
            except Exception as e:
                self.logger.debug(f"Test query failed (expected): {e}")
            
            return {
                'table_accessible': table_status == 'ACTIVE',
                'table_status': table_status,
                'test_query_successful': test_query_successful,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
        except Exception as e:
            self.logger.error(f"Historical data health check failed: {e}")
            return {
                'table_accessible': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }