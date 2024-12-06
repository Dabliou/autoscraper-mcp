import sqlite3
import json
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd

from .base import StorageBackend

class SQLiteStorage(StorageBackend):
    """SQLite storage backend for scraped data."""

    def __init__(self):
        super().__init__()
        self._connections: Dict[str, sqlite3.Connection] = {}

    async def _get_connection(self, path: str) -> sqlite3.Connection:
        """Get or create SQLite connection.
        
        Args:
            path: Path to SQLite database
            
        Returns:
            SQLite connection
        """
        if path not in self._connections:
            self._connections[path] = sqlite3.connect(path)
            # Enable foreign keys and case-sensitive strings
            self._connections[path].execute('PRAGMA foreign_keys = ON')
            self._connections[path].execute('PRAGMA case_sensitive_like = ON')
        return self._connections[path]

    async def _create_table(self, conn: sqlite3.Connection, table_name: str, data: List[Dict]) -> None:
        """Create table based on data structure.
        
        Args:
            conn: SQLite connection
            table_name: Name of table to create
            data: Sample data to infer schema from
        """
        if not data:
            raise ValueError('Cannot create table from empty data')

        # Infer schema from first record
        schema = []
        for key, value in data[0].items():
            if isinstance(value, (int, bool)):
                dtype = 'INTEGER'
            elif isinstance(value, float):
                dtype = 'REAL'
            elif isinstance(value, dict):
                dtype = 'TEXT'  # Store JSON as text
            elif isinstance(value, list):
                dtype = 'TEXT'  # Store JSON as text
            else:
                dtype = 'TEXT'
            schema.append(f'"{key}" {dtype}')

        # Create table
        schema_sql = f'''CREATE TABLE IF NOT EXISTS "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {','.join(schema)}
        )'''
        conn.execute(schema_sql)
        conn.commit()

    async def save(self, data: List[Dict], path: str, table_name: str = 'scraped_data', **kwargs) -> None:
        """Save data to SQLite database.
        
        Args:
            data: List of dictionaries to save
            path: Path to SQLite database
            table_name: Name of table to save to
            **kwargs: Additional parameters
        """
        if not data:
            self.logger.warning('No data to save')
            return

        async def _save():
            path_obj = self._validate_path(path)
            conn = await self._get_connection(str(path_obj))
            
            # Create table if doesn't exist
            await self._create_table(conn, table_name, data)
            
            # Prepare data for insertion
            processed_data = []
            for record in data:
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, (dict, list)):
                        processed_record[key] = json.dumps(value)
                    else:
                        processed_record[key] = value
                processed_data.append(processed_record)
            
            # Insert data using pandas for efficiency
            df = pd.DataFrame(processed_data)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            
            conn.commit()

        await self._handle_operation('save to SQLite', _save)

    async def load(self, path: str, table_name: str = 'scraped_data', **kwargs) -> List[Dict]:
        """Load data from SQLite database.
        
        Args:
            path: Path to SQLite database
            table_name: Name of table to load from
            **kwargs: Additional parameters
            
        Returns:
            List of dictionaries with loaded data
        """
        async def _load():
            path_obj = self._validate_path(path)
            conn = await self._get_connection(str(path_obj))
            
            # Load data using pandas
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
            
            # Process JSON columns
            records = df.to_dict('records')
            for record in records:
                for key, value in record.items():
                    if isinstance(value, str):
                        try:
                            record[key] = json.loads(value)
                        except json.JSONDecodeError:
                            pass
            
            return records

        return await self._handle_operation('load from SQLite', _load)

    async def append(self, data: List[Dict], path: str, table_name: str = 'scraped_data', **kwargs) -> None:
        """Append data to SQLite database.
        
        Args:
            data: List of dictionaries to append
            path: Path to SQLite database
            table_name: Name of table to append to
            **kwargs: Additional parameters
        """
        if not data:
            self.logger.warning('No data to append')
            return

        async def _append():
            path_obj = self._validate_path(path)
            conn = await self._get_connection(str(path_obj))
            
            # Create table if doesn't exist
            await self._create_table(conn, table_name, data)
            
            # Prepare data for insertion
            processed_data = []
            for record in data:
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, (dict, list)):
                        processed_record[key] = json.dumps(value)
                    else:
                        processed_record[key] = value
                processed_data.append(processed_record)
            
            # Append data using pandas
            df = pd.DataFrame(processed_data)
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            conn.commit()

        await self._handle_operation('append to SQLite', _append)

    async def exists(self, path: str) -> bool:
        """Check if SQLite database exists.
        
        Args:
            path: Path to check
            
        Returns:
            True if exists, False otherwise
        """
        try:
            path_obj = Path(path)
            return path_obj.exists()
        except Exception as e:
            self.logger.error(f'Error checking existence of {path}: {str(e)}')
            return False

    def __del__(self):
        """Close all connections on cleanup."""
        for conn in self._connections.values():
            try:
                conn.close()
            except Exception as e:
                self.logger.error(f'Error closing connection: {str(e)}')
