import csv
import json
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd

from .base import StorageBackend

class CSVStorage(StorageBackend):
    """CSV storage backend for scraped data."""

    def __init__(self):
        super().__init__()

    def _process_data(self, data: List[Dict]) -> pd.DataFrame:
        """Process data into a format suitable for CSV storage.
        
        Args:
            data: List of dictionaries to process
            
        Returns:
            Processed pandas DataFrame
        """
        processed_data = []
        for record in data:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    processed_record[key] = json.dumps(value)
                else:
                    processed_record[key] = value
            processed_data.append(processed_record)
        return pd.DataFrame(processed_data)

    async def save(self, data: List[Dict], path: str, **kwargs) -> None:
        """Save data to CSV file.
        
        Args:
            data: List of dictionaries to save
            path: Path to CSV file
            **kwargs: Additional parameters including:
                     - encoding: File encoding (default: utf-8)
                     - index: Include index (default: False)
        """
        if not data:
            self.logger.warning('No data to save')
            return

        async def _save():
            path_obj = self._validate_path(path)
            df = self._process_data(data)
            
            encoding = kwargs.get('encoding', 'utf-8')
            index = kwargs.get('index', False)
            
            df.to_csv(
                path_obj,
                encoding=encoding,
                index=index,
                quoting=csv.QUOTE_NONNUMERIC
            )

        await self._handle_operation('save to CSV', _save)

    async def load(self, path: str, **kwargs) -> List[Dict]:
        """Load data from CSV file.
        
        Args:
            path: Path to CSV file
            **kwargs: Additional parameters including:
                     - encoding: File encoding (default: utf-8)
                     
        Returns:
            List of dictionaries with loaded data
        """
        async def _load():
            path_obj = self._validate_path(path)
            encoding = kwargs.get('encoding', 'utf-8')
            
            df = pd.read_csv(
                path_obj,
                encoding=encoding,
                keep_default_na=False
            )
            
            records = df.to_dict('records')
            
            # Process JSON strings back to objects
            for record in records:
                for key, value in record.items():
                    if isinstance(value, str):
                        try:
                            record[key] = json.loads(value)
                        except json.JSONDecodeError:
                            pass
            
            return records

        return await self._handle_operation('load from CSV', _load)

    async def append(self, data: List[Dict], path: str, **kwargs) -> None:
        """Append data to CSV file.
        
        Args:
            data: List of dictionaries to append
            path: Path to CSV file
            **kwargs: Additional parameters including:
                     - encoding: File encoding (default: utf-8)
        """
        if not data:
            self.logger.warning('No data to append')
            return

        async def _append():
            path_obj = self._validate_path(path)
            df = self._process_data(data)
            
            encoding = kwargs.get('encoding', 'utf-8')
            
            # If file exists, append without headers
            header = not path_obj.exists()
            
            df.to_csv(
                path_obj,
                mode='a',
                header=header,
                encoding=encoding,
                index=False,
                quoting=csv.QUOTE_NONNUMERIC
            )

        await self._handle_operation('append to CSV', _append)

    async def exists(self, path: str) -> bool:
        """Check if CSV file exists.
        
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