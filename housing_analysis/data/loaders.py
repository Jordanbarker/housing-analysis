"""Unified data loader interface."""

from pathlib import Path
from typing import Optional, Union
import pandas as pd

from ..config import config
from .fred import FREDLoader
from .newyorkfed import NYFedLoader
from .census import CensusLoader


class DataManager:
    """High-level interface for all data loaders."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize data manager.
        
        Parameters
        ----------
        cache_dir : Optional[Path]
            Directory for caching downloaded data.
        """
        self.cache_dir = cache_dir or config.DATA_DIR / 'cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize loaders
        self.fred = FREDLoader(cache_dir=self.cache_dir)
        self.nyfed = NYFedLoader(cache_dir=self.cache_dir)
        self.census = CensusLoader(cache_dir=self.cache_dir)
    
    def load_recessions(self) -> pd.DataFrame:
        """Load US recession data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame with recession start and end dates.
        """
        path = config.DATA_DIR / 'US_recessions.xlsx'
        if not path.exists():
            raise FileNotFoundError(f"Recession data not found at {path}")
        
        recessions = pd.read_excel(path)
        recessions['Start'] = pd.to_datetime(recessions['Start'])
        recessions['End'] = pd.to_datetime(recessions['End'])
        return recessions
    
    # FRED convenience methods
    def load_treasury_spread(self, **kwargs) -> pd.DataFrame:
        """Load 10-Year Treasury minus 3-Month Treasury spread."""
        return self.fred.load_treasury_spread(**kwargs)
    
    def load_unemployment_rate(self, **kwargs) -> pd.DataFrame:
        """Load unemployment rate."""
        return self.fred.load_unemployment_rate(**kwargs)
    
    def load_median_house_price(self, **kwargs) -> pd.DataFrame:
        """Load median house price."""
        return self.fred.load_median_house_price(**kwargs)
    
    def load_mortgage_rates(self, **kwargs) -> pd.DataFrame:
        """Load 30-year mortgage rates."""
        return self.fred.load_mortgage_rates(**kwargs)
    
    def load_case_shiller(self, **kwargs) -> pd.DataFrame:
        """Load Case-Shiller home price index."""
        return self.fred.load_case_shiller(**kwargs)
    
    # NY Fed convenience methods  
    def load_loan_report(self, **kwargs):
        """Load total debt balance and composition."""
        return self.nyfed.load_loan_report(**kwargs)
    
    def load_foreclosures(self, **kwargs):
        """Load foreclosure data."""
        return self.nyfed.load_foreclosures(**kwargs)
    
    def load_mortgage_credit_scores(self, **kwargs):
        """Load mortgage originations by credit score."""
        return self.nyfed.load_mortgage_credit_scores(**kwargs)