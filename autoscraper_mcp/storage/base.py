from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    def __init__(self):
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    @abstractmethod
    async def save(self, data: Any, path: str, **kwargs) -> None:
        """Save data to storage.
        
        Args:
            data: Data to store
            path: Storage location
            **kwargs: Additional backend-specific parameters
        """
        pass

    @abstractmethod
    async def load(self, path: str, **kwargs) -> Any:
        """Load data from storage.
        
        Args:
            path: Storage location
            **kwargs: Additional backend-specific parameters
            
        Returns:
            Loaded data
        """
        pass

    @abstractmethod
    async def append(self, data: Any, path: str, **kwargs) -> None:
        """Append data to existing storage.
        
        Args:
            data: Data to append
            path: Storage location
            **kwargs: Additional backend-specific parameters
        """
        pass

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if storage location exists.
        
        Args:
            path: Storage location to check
            
        Returns:
            True if exists, False otherwise
        """
        pass

    def _validate_path(self, path: str) -> Path:
        """Validate and convert path string to Path object.
        
        Args:
            path: Path string to validate
            
        Returns:
            Validated Path object
            
        Raises:
            ValueError: If path is invalid
        """
        try:
            path_obj = Path(path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            return path_obj
        except Exception as e:
            self.logger.error(f'Invalid path {path}: {str(e)}')
            raise ValueError(f'Invalid path {path}: {str(e)}')

    async def _handle_operation(self, operation_name: str, func, *args, **kwargs) -> Any:
        """Handle storage operation with logging and error handling.
        
        Args:
            operation_name: Name of the operation for logging
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result of func execution
            
        Raises:
            Exception: If operation fails
        """
        try:
            self.logger.info(f'Starting {operation_name}')
            result = await func(*args, **kwargs)
            self.logger.info(f'Completed {operation_name}')
            return result
        except Exception as e:
            self.logger.error(f'Error in {operation_name}: {str(e)}')
            raise