"""Integration tests for the refactored housing analysis package."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, Mock
from housing_analysis import Datasets, DataManager


def test_backward_compatibility():
    """Test that old Datasets class still works."""
    datasets = Datasets()
    assert hasattr(datasets, 'load_stlouisfed_data')
    assert hasattr(datasets, 'load_newyorkfed_data')
    assert hasattr(datasets, 'load_recessions')


def test_new_data_manager():
    """Test new DataManager interface."""
    with patch('housing_analysis.data.loaders.config') as mock_config:
        mock_config.DATA_DIR = Path('/tmp')
        mock_config.FRED_API_KEY = 'test_key'
        
        manager = DataManager()
        assert hasattr(manager, 'fred')
        assert hasattr(manager, 'nyfed')
        assert hasattr(manager, 'census')
        
        # Test that convenience methods exist
        assert hasattr(manager, 'load_treasury_spread')
        assert hasattr(manager, 'load_unemployment_rate')
        assert hasattr(manager, 'load_loan_report')


def test_imports():
    """Test that all new modules can be imported."""
    from housing_analysis import config
    from housing_analysis import utils
    from housing_analysis import visualization
    from housing_analysis import exceptions
    from housing_analysis.data import FREDLoader, NYFedLoader
    
    # Test specific functions
    from housing_analysis.utils import calculate_mortgage_payment
    from housing_analysis.visualization import plot_time_series
    
    assert config is not None
    assert utils is not None
    assert visualization is not None
    assert exceptions is not None
    assert FREDLoader is not None
    assert NYFedLoader is not None
    assert calculate_mortgage_payment is not None
    assert plot_time_series is not None


def test_config_singleton():
    """Test that config is accessible as a singleton."""
    from housing_analysis.config import config
    
    assert config is not None
    assert hasattr(config, 'FRED_API_KEY')
    assert hasattr(config, 'DATA_DIR')
    assert hasattr(config, 'FRED_BASE_URL')