"""FRED API data loader."""

from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
import requests
from datetime import datetime

from ..config import config
from ..exceptions import APIError, DataLoadError
from .base import DataLoader


class FREDLoader(DataLoader):
    """Loader for Federal Reserve Economic Data (FRED)."""
    
    # Map of friendly names to FRED series IDs
    SERIES_MAP = {
        'treasury_spread': 'T10Y3M',
        'active_listings': 'ACTLISCOUUS',
        'new_listings': 'NEWLISCOUUS',
        'new_house_supply': 'MSACSR',
        'new_house_started': 'HOUST',
        'unemployment_rate': 'UNRATE',
        'median_days_on_market': 'MEDDAYONMARUS',
        'median_house_price': 'MSPUS',
        'median_household_income': 'MEHOINUSA672N',
        'case_shiller': 'CSUSHPINSA',
        'mortgage_rates': 'MORTGAGE30US',
    }
    
    def __init__(self, api_key: Optional[str] = None, cache_dir: Optional[Path] = None):
        """Initialize FRED loader.
        
        Parameters
        ----------
        api_key : Optional[str]
            FRED API key. If not provided, uses config.
        cache_dir : Optional[Path]
            Directory for caching downloaded data.
        """
        super().__init__(cache_dir)
        self.api_key = api_key or config.FRED_API_KEY
        if not self.api_key:
            raise ValueError("FRED API key not provided")
        self.base_url = config.FRED_BASE_URL
    
    def load(self, series_id: str, start_date: Optional[str] = None, 
             end_date: Optional[str] = None, use_cache: bool = True) -> pd.DataFrame:
        """Load data from FRED API.
        
        Parameters
        ----------
        series_id : str
            FRED series ID or friendly name.
        start_date : Optional[str]
            Start date in YYYY-MM-DD format.
        end_date : Optional[str]
            End date in YYYY-MM-DD format.
        use_cache : bool
            Whether to use cached data if available.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with 'date' and 'value' columns.
        """
        # Convert friendly name to series ID if needed
        if series_id in self.SERIES_MAP:
            series_id = self.SERIES_MAP[series_id]
        
        # Check cache first
        if use_cache and self.cache_dir:
            cached_data = self._load_from_cache(series_id)
            if cached_data is not None:
                return self._filter_dates(cached_data, start_date, end_date)
        
        # Fetch from API
        try:
            params = {
                'series_id': series_id,
                'api_key': self.api_key,
                'file_type': 'json',
            }
            
            if start_date:
                params['observation_start'] = start_date
            if end_date:
                params['observation_end'] = end_date
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'error_code' in data:
                raise APIError(f"FRED API error: {data.get('error_message', 'Unknown error')}")
            
            # Convert to DataFrame
            observations = data.get('observations', [])
            if not observations:
                raise DataLoadError(f"No data returned for series {series_id}")
            
            df = pd.DataFrame(observations)
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[['date', 'value']].dropna()
            
            # Cache the data
            if self.cache_dir:
                self._save_to_cache(df, series_id)
            
            return df
            
        except requests.RequestException as e:
            raise APIError(f"Failed to fetch data from FRED: {e}")
        except Exception as e:
            raise DataLoadError(f"Error loading FRED data: {e}")
    
    def get_available_series(self) -> List[str]:
        """Get list of available data series.
        
        Returns
        -------
        List[str]
            List of available series names.
        """
        return list(self.SERIES_MAP.keys())
    
    def _filter_dates(self, df: pd.DataFrame, start_date: Optional[str], 
                      end_date: Optional[str]) -> pd.DataFrame:
        """Filter DataFrame by date range.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to filter.
        start_date : Optional[str]
            Start date in YYYY-MM-DD format.
        end_date : Optional[str]
            End date in YYYY-MM-DD format.
            
        Returns
        -------
        pd.DataFrame
            Filtered DataFrame.
        """
        df = df.copy()
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        return df
    
    # Convenience methods for commonly used series
    def load_treasury_spread(self, **kwargs) -> pd.DataFrame:
        """Load 10-Year Treasury minus 3-Month Treasury spread."""
        return self.load('treasury_spread', **kwargs)
    
    def load_unemployment_rate(self, **kwargs) -> pd.DataFrame:
        """Load unemployment rate."""
        return self.load('unemployment_rate', **kwargs)
    
    def load_median_house_price(self, **kwargs) -> pd.DataFrame:
        """Load median house price."""
        return self.load('median_house_price', **kwargs)
    
    def load_mortgage_rates(self, **kwargs) -> pd.DataFrame:
        """Load 30-year mortgage rates."""
        return self.load('mortgage_rates', **kwargs)
    
    def load_case_shiller(self, **kwargs) -> pd.DataFrame:
        """Load Case-Shiller home price index."""
        return self.load('case_shiller', **kwargs)