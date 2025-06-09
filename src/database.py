"""DynamoDB operations for the NASDAQ-100 scraper."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import boto3
from boto3.dynamodb.conditions import Key

from src.config import config
from src.exceptions import DatabaseError, AuthenticationError
from src.models import StockData
from src.utils import retry_with_backoff


class DynamoDBManager:
    """Manages DynamoDB operations for stock data."""
    
    def __init__(self, table_name: str = None, region: str = None):
        self.table_name = table_name or config.DYNAMODB_TABLE_NAME
        self.region = region or config.AWS_REGION
        self.logger = logging.getLogger(__name__)
        
        try:
            # Initialize DynamoDB resources
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.client = boto3.client('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)
            
            self.logger.info(f"DynamoDB manager initialized for table: {self.table_name}")
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AuthenticationError(f"AWS credentials not found or incomplete: {e}")
        except Exception as e:
            raise DatabaseError(f"Failed to initialize DynamoDB: {e}")
    
    def test_connection(self) -> bool:
        """Test DynamoDB connection and table access."""
        try:
            # Try to describe the table
            response = self.table.meta.client.describe_table(TableName=self.table_name)
            table_status = response['Table']['TableStatus']
            
            if table_status == 'ACTIVE':
                self.logger.info(f"DynamoDB connection successful. Table status: {table_status}")
                return True
            else:
                self.logger.warning(f"Table exists but status is: {table_status}")
                return False
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'ResourceNotFoundException':
                self.logger.error(f"Table {self.table_name} does not exist")
                return False
            elif error_code == 'AccessDeniedException':
                self.logger.error("Access denied to DynamoDB table")
                return False
            else:
                self.logger.error(f"DynamoDB connection test failed: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Unexpected error testing DynamoDB connection: {e}")
            return False
    
    def create_table_if_not_exists(self) -> bool:
        """Create the stocks table if it doesn't exist."""
        try:
            # Check if table exists
            self.table.meta.client.describe_table(TableName=self.table_name)
            self.logger.info(f"Table {self.table_name} already exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise DatabaseError(f"Error checking table existence: {e}")
        
        try:
            # Create table
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'symbol',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'symbol',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'  # On-demand billing
            )
            
            # Wait for table to be created
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.logger.info(f"Successfully created table: {self.table_name}")
            return True
            
        except ClientError as e:
            raise DatabaseError(f"Failed to create table: {e}")
    
    @retry_with_backoff(max_retries=3)
    def save_stock_data(self, stock_data: StockData) -> bool:
        """Save or update stock data in DynamoDB."""
        try:
            # Convert StockData to DynamoDB item
            item = self._stock_data_to_item(stock_data)
            
            # Put item in table
            response = self.table.put_item(Item=item)
            
            # Check response
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.debug(f"Successfully saved data for {stock_data.symbol}")
                return True
            else:
                self.logger.warning(f"Unexpected response saving {stock_data.symbol}: {response}")
                return False
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"DynamoDB error saving {stock_data.symbol}: {error_code} - {e}")
            raise DatabaseError(f"Failed to save stock data for {stock_data.symbol}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error saving {stock_data.symbol}: {e}")
            raise DatabaseError(f"Unexpected error saving stock data: {e}")
    
    def save_batch_stock_data(self, stock_data_list: List[StockData]) -> Tuple[int, List[str]]:
        """Save multiple stock data items using batch write."""
        if not stock_data_list:
            return 0, []
        
        successful_count = 0
        failed_symbols = []
        
        # DynamoDB batch_writer handles batching automatically (max 25 items per batch)
        try:
            with self.table.batch_writer() as batch:
                for stock_data in stock_data_list:
                    try:
                        item = self._stock_data_to_item(stock_data)
                        batch.put_item(Item=item)
                        successful_count += 1
                        self.logger.debug(f"Batch queued data for {stock_data.symbol}")
                        
                    except Exception as e:
                        self.logger.error(f"Error preparing batch item for {stock_data.symbol}: {e}")
                        failed_symbols.append(stock_data.symbol)
            
            self.logger.info(f"Batch write completed: {successful_count} successful, {len(failed_symbols)} failed")
            
        except ClientError as e:
            self.logger.error(f"Batch write failed: {e}")
            # If batch fails, try individual saves for remaining items
            return self._fallback_individual_saves(stock_data_list)
        
        return successful_count, failed_symbols
    
    def _fallback_individual_saves(self, stock_data_list: List[StockData]) -> Tuple[int, List[str]]:
        """Fallback to individual saves if batch write fails."""
        self.logger.info("Falling back to individual saves")
        successful_count = 0
        failed_symbols = []
        
        for stock_data in stock_data_list:
            try:
                if self.save_stock_data(stock_data):
                    successful_count += 1
                else:
                    failed_symbols.append(stock_data.symbol)
            except DatabaseError:
                failed_symbols.append(stock_data.symbol)
        
        return successful_count, failed_symbols
    
    def get_stock_data(self, symbol: str) -> Optional[StockData]:
        """Retrieve stock data for a specific symbol."""
        try:
            response = self.table.get_item(Key={'symbol': symbol.upper()})
            
            if 'Item' in response:
                return self._item_to_stock_data(response['Item'])
            else:
                self.logger.debug(f"No data found for symbol: {symbol}")
                return None
                
        except ClientError as e:
            self.logger.error(f"Error retrieving data for {symbol}: {e}")
            raise DatabaseError(f"Failed to retrieve stock data for {symbol}: {e}")
    
    def get_all_stocks(self) -> List[StockData]:
        """Retrieve all stock data from the table."""
        try:
            stocks = []
            
            # Scan the entire table
            response = self.table.scan()
            for item in response['Items']:
                stocks.append(self._item_to_stock_data(item))
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                for item in response['Items']:
                    stocks.append(self._item_to_stock_data(item))
            
            self.logger.info(f"Retrieved {len(stocks)} stock records")
            return stocks
            
        except ClientError as e:
            self.logger.error(f"Error scanning table: {e}")
            raise DatabaseError(f"Failed to retrieve all stock data: {e}")
    
    def get_stocks_by_symbols(self, symbols: List[str]) -> Dict[str, Optional[StockData]]:
        """Retrieve stock data for multiple symbols using batch get."""
        if not symbols:
            return {}
        
        result = {}
        
        try:
            # DynamoDB batch_get_item has a limit of 100 items
            symbol_chunks = [symbols[i:i+100] for i in range(0, len(symbols), 100)]
            
            for chunk in symbol_chunks:
                request_items = {
                    self.table_name: {
                        'Keys': [{'symbol': symbol.upper()} for symbol in chunk]
                    }
                }
                
                response = self.client.batch_get_item(RequestItems=request_items)
                
                # Process returned items
                if self.table_name in response['Responses']:
                    for item in response['Responses'][self.table_name]:
                        stock_data = self._item_to_stock_data(item)
                        result[stock_data.symbol] = stock_data
                
                # Handle unprocessed keys
                while 'UnprocessedKeys' in response and response['UnprocessedKeys']:
                    response = self.client.batch_get_item(
                        RequestItems=response['UnprocessedKeys']
                    )
                    if self.table_name in response['Responses']:
                        for item in response['Responses'][self.table_name]:
                            stock_data = self._item_to_stock_data(item)
                            result[stock_data.symbol] = stock_data
            
            # Add None for symbols not found
            for symbol in symbols:
                if symbol.upper() not in result:
                    result[symbol.upper()] = None
            
            self.logger.info(f"Retrieved data for {len([v for v in result.values() if v])} out of {len(symbols)} symbols")
            return result
            
        except ClientError as e:
            self.logger.error(f"Error in batch get: {e}")
            raise DatabaseError(f"Failed to retrieve stock data: {e}")
    
    def delete_stock_data(self, symbol: str) -> bool:
        """Delete stock data for a specific symbol."""
        try:
            response = self.table.delete_item(Key={'symbol': symbol.upper()})
            
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.info(f"Successfully deleted data for {symbol}")
                return True
            else:
                self.logger.warning(f"Unexpected response deleting {symbol}: {response}")
                return False
                
        except ClientError as e:
            self.logger.error(f"Error deleting data for {symbol}: {e}")
            raise DatabaseError(f"Failed to delete stock data for {symbol}: {e}")
    
    def get_table_stats(self) -> Dict[str, Any]:
        """Get table statistics and information."""
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
            
            stats = {
                'table_name': self.table_name,
                'table_status': table_info['TableStatus'],
                'item_count': item_count,
                'table_size_bytes': table_info.get('TableSizeBytes', 0),
                'creation_datetime': table_info.get('CreationDateTime'),
                'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode', 'Unknown'),
                'last_updated': datetime.utcnow().isoformat() + 'Z'
            }
            
            return stats
            
        except ClientError as e:
            self.logger.error(f"Error getting table stats: {e}")
            raise DatabaseError(f"Failed to get table statistics: {e}")
    
    def _stock_data_to_item(self, stock_data: StockData) -> Dict[str, Any]:
        """Convert StockData object to DynamoDB item."""
        return {
            'symbol': stock_data.symbol,
            'price': stock_data.price,
            'daily_change_percent': stock_data.daily_change_percent,
            'daily_change_nominal': stock_data.daily_change_nominal,
            'volume': stock_data.volume,
            'high': stock_data.high,
            'low': stock_data.low,
            'open': stock_data.open,                    # NEW FIELD
            'previous_close': stock_data.previous_close, # NEW FIELD
            'last_updated': stock_data.last_updated,
            'market': stock_data.market
        }
    
    def _item_to_stock_data(self, item: Dict[str, Any]) -> StockData:
        """Convert DynamoDB item to StockData object."""
        return StockData(
            symbol=item['symbol'],
            price=Decimal(str(item['price'])),
            daily_change_percent=Decimal(str(item['daily_change_percent'])),
            daily_change_nominal=Decimal(str(item['daily_change_nominal'])),
            volume=int(item['volume']),
            high=Decimal(str(item['high'])),
            low=Decimal(str(item['low'])),
            open=Decimal(str(item.get('open', item['price']))),                    # NEW FIELD with fallback
            previous_close=Decimal(str(item.get('previous_close', item['price']))), # NEW FIELD with fallback
            last_updated=item['last_updated'],
            market=item.get('market', 'NASDAQ')
        )
    
    def health_check(self) -> Dict[str, Any]:
        """Perform database health check."""
        try:
            # Test basic connectivity
            connection_ok = self.test_connection()
            
            # Get basic stats
            stats = self.get_table_stats() if connection_ok else {}
            
            # Test a simple operation
            test_symbol = 'AAPL'
            test_read_ok = False
            try:
                self.get_stock_data(test_symbol)
                test_read_ok = True
            except Exception as e:
                self.logger.debug(f"Test read failed: {e}")
            
            return {
                'database_connection': connection_ok,
                'table_accessible': connection_ok,
                'test_read_successful': test_read_ok,
                'item_count': stats.get('item_count', 0),
                'table_status': stats.get('table_status', 'Unknown'),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return {
                'database_connection': False,
                'table_accessible': False,
                'test_read_successful': False,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }