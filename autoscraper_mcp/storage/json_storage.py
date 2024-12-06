import json
from typing import Any, Dict, List, Optional
from pathlib import Path
import aiofiles
import ijson

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
                     - chunk_size: Size of chunks for large files (default: 8192)
            
        Returns:
            List of dictionaries with loaded data
        """
        async def _load():
            path_obj = self._validate_path(path)
            encoding = kwargs.get('encoding', 'utf-8')
            chunk_size = kwargs.get('chunk_size', 8192)
            
            # Use ijson for streaming large files
            if path_obj.stat().st_size > 10 * 1024 * 1024:  # 10MB
                self.logger.info('Large file detected, using streaming parser')
                async with aiofiles.open(path_obj, mode='rb') as f:
                    content = await f.read()
                    parser = ijson.parse(content)
                    data = []
                    current_item = None
                    
                    for prefix, event, value in parser:
                        if prefix == 'item' and event == 'start_map':
                            current_item = {}
                        elif prefix.startswith('item.') and current_item is not None:
                            key = prefix.split('.')[1]
                            current_item[key] = value
                        elif prefix == 'item' and event == 'end_map':
                            data.append(current_item)
                            current_item = None
                    
                    return data
            else:
                # Regular loading for smaller files
                async with aiofiles.open(path_obj, encoding=encoding) as f:
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
            encoding = kwargs.get('encoding', 'utf-8')
            
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