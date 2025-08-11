"""Base classes for data loaders."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd


class DataLoader(ABC):
    """Abstract base class for data loaders."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize data loader.
        
        Parameters
        ----------
        cache_dir : Optional[Path]
            Directory for caching downloaded data.
        """
        self.cache_dir = cache_dir
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def load(self, **kwargs) -> pd.DataFrame:
        """Load data from source.
        
        Returns
        -------
        pd.DataFrame
            Loaded data.
        """
        pass
    
    @abstractmethod
    def get_available_series(self) -> List[str]:
        """Get list of available data series.
        
        Returns
        -------
        List[str]
            List of available series identifiers.
        """
        pass
    
    def _get_cache_path(self, identifier: str) -> Path:
        """Get cache file path for given identifier.
        
        Parameters
        ----------
        identifier : str
            Data identifier.
            
        Returns
        -------
        Path
            Path to cache file.
        """
        if not self.cache_dir:
            raise ValueError("Cache directory not set")
        return self.cache_dir / f"{identifier}.parquet"
    
    def _load_from_cache(self, identifier: str) -> Optional[pd.DataFrame]:
        """Load data from cache if available.
        
        Parameters
        ----------
        identifier : str
            Data identifier.
            
        Returns
        -------
        Optional[pd.DataFrame]
            Cached data if available, None otherwise.
        """
        if not self.cache_dir:
            return None
            
        cache_path = self._get_cache_path(identifier)
        if cache_path.exists():
            return pd.read_parquet(cache_path)
        return None
    
    def _save_to_cache(self, data: pd.DataFrame, identifier: str) -> None:
        """Save data to cache.
        
        Parameters
        ----------
        data : pd.DataFrame
            Data to cache.
        identifier : str
            Data identifier.
        """
        if self.cache_dir:
            cache_path = self._get_cache_path(identifier)
            data.to_parquet(cache_path)