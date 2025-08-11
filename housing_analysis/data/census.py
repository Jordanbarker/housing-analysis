"""Census data loader (placeholder for future implementation)."""

from pathlib import Path
from typing import Optional, List
import pandas as pd

from ..config import config
from ..exceptions import DataLoadError
from .base import DataLoader


class CensusLoader(DataLoader):
    """Loader for US Census data."""
    
    def __init__(self, api_key: Optional[str] = None, cache_dir: Optional[Path] = None):
        """Initialize Census loader.
        
        Parameters
        ----------
        api_key : Optional[str]
            Census API key (if required).
        cache_dir : Optional[Path]
            Directory for caching downloaded data.
        """
        super().__init__(cache_dir)
        self.api_key = api_key
        self.base_url = config.CENSUS_API_BASE
    
    def load(self, **kwargs) -> pd.DataFrame:
        """Load Census data.
        
        Returns
        -------
        pd.DataFrame
            Census data.
        """
        # Placeholder for future Census API implementation
        raise NotImplementedError("Census data loading not yet implemented")
    
    def get_available_series(self) -> List[str]:
        """Get list of available data series.
        
        Returns
        -------
        List[str]
            List of available Census data series.
        """
        # Placeholder - will be populated when Census integration is added
        return []