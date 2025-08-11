"""New York Fed data loader."""

from pathlib import Path
from typing import Optional, List, Tuple
import pandas as pd
import requests

from ..config import config
from ..exceptions import DataLoadError
from .base import DataLoader


class NYFedLoader(DataLoader):
    """Loader for New York Fed household debt and credit data."""
    
    # Sheet configurations for different data types
    SHEET_CONFIG = {
        'loan_report': {'sheet_name': 'Page 3 Data', 'header': 3},
        'loan_accounts': {'sheet_name': 'Page 4 Data', 'header': 3},
        'mortgage_credit_scores': {'sheet_name': 'Page 6 Data', 'header': 3},
        'auto_credit_scores': {'sheet_name': 'Page 8 Data', 'header': 3},
        'delinquent_loans': {'sheet_name': 'Page 13 Data', 'header': 4},
        'foreclosures': {'sheet_name': 'Page 17 Data', 'header': 3},
    }
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize NY Fed loader.
        
        Parameters
        ----------
        cache_dir : Optional[Path]
            Directory for caching downloaded data.
        """
        super().__init__(cache_dir)
        self.data_url = config.NY_FED_MORTGAGE_URL
        self.local_file = config.DATA_DIR / 'HHD_C_Report_2024Q4.xlsx'
    
    def load(self, data_type: str, use_cache: bool = True) -> Tuple[pd.DataFrame, str, str]:
        """Load data from NY Fed.
        
        Parameters
        ----------
        data_type : str
            Type of data to load (e.g., 'loan_report', 'foreclosures').
        use_cache : bool
            Whether to use cached data if available.
            
        Returns
        -------
        Tuple[pd.DataFrame, str, str]
            DataFrame with data, title, and value label.
        """
        if data_type not in self.SHEET_CONFIG:
            raise ValueError(f"Unknown data type: {data_type}")
        
        # Check cache first
        if use_cache and self.cache_dir:
            cached_data = self._load_from_cache(data_type)
            if cached_data is not None:
                # For NY Fed data, we cache with metadata
                title = cached_data.attrs.get('title', '')
                value_label = cached_data.attrs.get('value_label', '')
                return cached_data, title, value_label
        
        # Download file if not exists
        if not self.local_file.exists():
            self._download_data()
        
        # Load from Excel
        config_info = self.SHEET_CONFIG[data_type]
        
        try:
            # Read header information
            header_df = pd.read_excel(
                self.local_file,
                sheet_name=config_info['sheet_name'],
                header=0,
                nrows=2
            )
            title = str(header_df.columns[0])
            value_label = str(header_df.iloc[0, 0])
            if '*' in title:
                title += f' ({header_df.iloc[1, 0]})'
            
            # Read actual data
            df = pd.read_excel(
                self.local_file,
                sheet_name=config_info['sheet_name'],
                header=config_info['header']
            )
            
            # Process data
            df = self._process_data(df)
            
            # Cache with metadata
            if self.cache_dir:
                df.attrs['title'] = title
                df.attrs['value_label'] = value_label
                self._save_to_cache(df, data_type)
            
            return df, title, value_label
            
        except Exception as e:
            raise DataLoadError(f"Error loading NY Fed data: {e}")
    
    def get_available_series(self) -> List[str]:
        """Get list of available data series.
        
        Returns
        -------
        List[str]
            List of available data types.
        """
        return list(self.SHEET_CONFIG.keys())
    
    def _download_data(self) -> None:
        """Download NY Fed data file."""
        try:
            response = requests.get(self.data_url, stream=True)
            response.raise_for_status()
            
            self.local_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.local_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        except requests.RequestException as e:
            raise DataLoadError(f"Failed to download NY Fed data: {e}")
    
    def _process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process NY Fed data.
        
        Parameters
        ----------
        df : pd.DataFrame
            Raw data from Excel.
            
        Returns
        -------
        pd.DataFrame
            Processed data.
        """
        # Clean date column
        df = self._clean_date_column(df)
        
        # Drop unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        return df
    
    def _clean_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and convert date column.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with date column.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with cleaned date column.
        """
        df = df.copy()
        df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
        df['Date'] = df['Date'].astype(str)
        df['Date'] = df['Date'].apply(self._convert_to_datetime)
        df.rename(columns={'Date': 'date'}, inplace=True)
        return df
    
    def _convert_to_datetime(self, date_str: str) -> pd.Timestamp:
        """Convert mixed date formats to datetime.
        
        Parameters
        ----------
        date_str : str
            Date string in various formats.
            
        Returns
        -------
        pd.Timestamp
            Converted datetime.
        """
        if ' ' not in date_str:
            # Handle quarter format (e.g., '11:Q2')
            year, quarter = date_str.split(':')
            year = '20' + year  # Assuming 2000s
            quarter_month = {
                'Q1': '03-01',
                'Q2': '06-01',
                'Q3': '09-01',
                'Q4': '12-01'
            }
            return pd.to_datetime(f'{year}-{quarter_month[quarter]}')
        else:
            # Standard date format
            return pd.to_datetime(date_str)
    
    # Convenience methods
    def load_loan_report(self, **kwargs) -> Tuple[pd.DataFrame, str, str]:
        """Load total debt balance and composition (in Trillions)."""
        return self.load('loan_report', **kwargs)
    
    def load_loan_accounts(self, **kwargs) -> Tuple[pd.DataFrame, str, str]:
        """Load number of accounts by loan type (in Millions)."""
        return self.load('loan_accounts', **kwargs)
    
    def load_mortgage_credit_scores(self, **kwargs) -> Tuple[pd.DataFrame, str, str]:
        """Load mortgage originations by credit score."""
        return self.load('mortgage_credit_scores', **kwargs)
    
    def load_foreclosures(self, **kwargs) -> Tuple[pd.DataFrame, str, str]:
        """Load foreclosure data."""
        return self.load('foreclosures', **kwargs)