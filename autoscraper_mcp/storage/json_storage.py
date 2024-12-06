import json
import os
from pathlib import Path
from typing import Any, Dict, List
import aiofiles

from .base import StorageBackend

class JSONStorage(StorageBackend):
    """JSON storage backend for scraped data."""

    def __init__(self):
        super().__init__()

    async def save(self, data: List[Dict], path: str, **kwargs) -> None:
        """Save data to JSON file.
        
        Args:
            data: List of dictionaries to save
            path: Path to JSON file
            **kwargs: Additional parameters including:
                     - indent: JSON indentation (default: 2)
                     - encoding: File encoding (default: utf-8)
        """
        if not data:
            self.logger.warning('No data to save')
            return

        async def _save():
            path_obj = self._validate_path(path)
            indent = kwargs.get('indent', 2)
            encoding = kwargs.get('encoding', 'utf-8')
            
            async with aiofiles.open(path_obj, mode='w', encoding=encoding) as f:
                # Use compact representation for very large datasets
                if len(data) > 10000:
                    self.logger.info('Large dataset detected, using compact JSON format')
                    await f.write('[\n')
                    for i, item in enumerate(data):
                        json_str = json.dumps(item)
                        if i < len(data) - 1:
                            json_str += ','
                        await f.write(json_str + '\n')
                    await f.write(']')
                else:
                    # Pretty print for smaller datasets
                    json_str = json.dumps(data, indent=indent)
                    await f.write(json_str)

        await self._handle_operation('save to JSON', _save)

    async def load(self, path: str, **kwargs) -> List[Dict]:
        """Load data from JSON file.
        
        Args:
            path: Path to JSON file
            **kwargs: Additional parameters including:
                     - encoding: File encoding (default: utf-8)
                     
        Returns:
            List of dictionaries with loaded data
        """
        async def _load():
            path_obj = self._validate_path(path)
            encoding = kwargs.get('encoding', 'utf-8')
            
            async with aiofiles.open(path_obj, mode='r', encoding=encoding) as f:
                content = await f.read()
                return json.loads(content)

        return await self._handle_operation('load from JSON', _load)

    async def append(self, data: List[Dict], path: str, **kwargs) -> None:
        """Append data to JSON file.
        
        Args:
            data: List of dictionaries to append
            path: Path to JSON file
            **kwargs: Additional parameters including:
                     - encoding: File encoding (default: utf-8)
        """
        if not data:
            self.logger.warning('No data to append')
            return

        async def _append():
            path_obj = self._validate_path(path)
            
            if not path_obj.exists():
                # If file doesn't exist, just save
                await self.save(data, str(path_obj), **kwargs)
                return
            
            # Load existing data
            existing_data = await self.load(str(path_obj))
            
            # Combine data
            combined_data = existing_data + data
            
            # Save combined data
            await self.save(combined_data, str(path_obj), **kwargs)

        await self._handle_operation('append to JSON', _append)

    async def exists(self, path: str) -> bool:
        """Check if JSON file exists.
        
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