"""Tests for unified data loader."""

import pytest
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, Mock
from housing_analysis.data.loaders import DataManager


def test_data_manager_initialization():
    """Test DataManager initialization."""
    with patch('housing_analysis.data.loaders.config') as mock_config:
        mock_config.DATA_DIR = Path('/tmp/test')
        mock_config.FRED_API_KEY = 'test_key'
        
        manager = DataManager()
        assert manager.cache_dir == Path('/tmp/test/cache')
        assert hasattr(manager, 'fred')
        assert hasattr(manager, 'nyfed')
        assert hasattr(manager, 'census')


def test_data_manager_with_custom_cache():
    """Test DataManager with custom cache directory."""
    with TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir) / 'custom_cache'
        with patch('housing_analysis.data.loaders.config') as mock_config:
            mock_config.FRED_API_KEY = 'test_key'
            
            manager = DataManager(cache_dir=cache_dir)
            assert manager.cache_dir == cache_dir
            assert cache_dir.exists()


def test_load_recessions():
    """Test loading recession data."""
    with patch('housing_analysis.data.loaders.config') as mock_config:
        mock_config.DATA_DIR = Path(__file__).parent
        mock_config.FRED_API_KEY = 'test_key'
        
        # Create test recession file
        with TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            mock_config.DATA_DIR = data_dir
            
            # Create test Excel file
            recession_data = pd.DataFrame({
                'Start': ['2020-01-01', '2021-01-01'],
                'End': ['2020-06-01', '2021-06-01']
            })
            recession_file = data_dir / 'US_recessions.xlsx'
            recession_data.to_excel(recession_file, index=False)
            
            manager = DataManager()
            recessions = manager.load_recessions()
            
            assert len(recessions) == 2
            assert pd.api.types.is_datetime64_any_dtype(recessions['Start'])
            assert pd.api.types.is_datetime64_any_dtype(recessions['End'])


def test_load_recessions_file_not_found():
    """Test recession loading when file doesn't exist."""
    with TemporaryDirectory() as tmpdir:
        with patch('housing_analysis.data.loaders.config') as mock_config:
            mock_config.DATA_DIR = Path(tmpdir) / 'nonexistent'
            mock_config.FRED_API_KEY = 'test_key'
            
            manager = DataManager(cache_dir=Path(tmpdir) / 'cache')
            with pytest.raises(FileNotFoundError):
                manager.load_recessions()


def test_fred_convenience_methods():
    """Test FRED convenience methods."""
    with patch('housing_analysis.data.loaders.config') as mock_config:
        mock_config.DATA_DIR = Path('/tmp')
        mock_config.FRED_API_KEY = 'test_key'
        
        manager = DataManager()
        
        # Mock the fred loader methods
        with patch.object(manager.fred, 'load_treasury_spread') as mock_method:
            mock_method.return_value = pd.DataFrame()
            manager.load_treasury_spread(start_date='2020-01-01')
            mock_method.assert_called_once_with(start_date='2020-01-01')
        
        with patch.object(manager.fred, 'load_unemployment_rate') as mock_method:
            mock_method.return_value = pd.DataFrame()
            manager.load_unemployment_rate()
            mock_method.assert_called_once()
        
        with patch.object(manager.fred, 'load_mortgage_rates') as mock_method:
            mock_method.return_value = pd.DataFrame()
            manager.load_mortgage_rates()
            mock_method.assert_called_once()


def test_nyfed_convenience_methods():
    """Test NY Fed convenience methods."""
    with patch('housing_analysis.data.loaders.config') as mock_config:
        mock_config.DATA_DIR = Path('/tmp')
        mock_config.FRED_API_KEY = 'test_key'
        
        manager = DataManager()
        
        # Mock the nyfed loader methods
        with patch.object(manager.nyfed, 'load_loan_report') as mock_method:
            mock_method.return_value = (pd.DataFrame(), 'Title', 'Label')
            result = manager.load_loan_report()
            mock_method.assert_called_once()
            assert len(result) == 3
        
        with patch.object(manager.nyfed, 'load_foreclosures') as mock_method:
            mock_method.return_value = (pd.DataFrame(), 'Title', 'Label')
            result = manager.load_foreclosures()
            mock_method.assert_called_once()
            assert len(result) == 3