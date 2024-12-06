from abc import ABC, abstractmethod
from typing import Any, Dict, List

class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    async def save(self, data: Any, path: str) -> None:
        """Save data to storage."""
        pass

    @abstractmethod
    async def load(self, path: str) -> Any:
        """Load data from storage."""
        pass

# TODO: Implement concrete storage backends for SQLite, CSV, JSON
