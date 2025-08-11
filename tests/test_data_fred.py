"""Tests for FRED data loader."""

import pytest
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, Mock
from housing_analysis.data.fred import FREDLoader
from housing_analysis.exceptions import APIError, DataLoadError


def test_fred_loader_initialization():
    """Test FREDLoader initialization."""
    # With API key
    loader = FREDLoader(api_key='test_key')
    assert loader.api_key == 'test_key'
    
    # Without API key (should use config)
    with patch('housing_analysis.data.fred.config') as mock_config:
        mock_config.FRED_API_KEY = 'config_key'
        mock_config.FRED_BASE_URL = 'https://api.test.com'
        loader = FREDLoader()
        assert loader.api_key == 'config_key'
    
    # No API key available
    with patch('housing_analysis.data.fred.config') as mock_config:
        mock_config.FRED_API_KEY = None
        with pytest.raises(ValueError, match="FRED API key"):
            FREDLoader()


def test_series_map():
    """Test series name mapping."""
    loader = FREDLoader(api_key='test_key')
    assert 'treasury_spread' in loader.SERIES_MAP
    assert loader.SERIES_MAP['treasury_spread'] == 'T10Y3M'
    assert 'mortgage_rates' in loader.SERIES_MAP


def test_get_available_series():
    """Test getting available series."""
    loader = FREDLoader(api_key='test_key')
    series = loader.get_available_series()
    assert isinstance(series, list)
    assert 'treasury_spread' in series
    assert 'unemployment_rate' in series


@patch('requests.get')
def test_load_data_success(mock_get):
    """Test successful data loading from API."""
    # Mock successful API response
    mock_response = Mock()
    mock_response.json.return_value = {
        'observations': [
            {'date': '2020-01-01', 'value': '3.5'},
            {'date': '2020-02-01', 'value': '3.6'},
            {'date': '2020-03-01', 'value': '3.4'},
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    loader = FREDLoader(api_key='test_key')
    df = loader.load('UNRATE')
    
    assert len(df) == 3
    assert 'date' in df.columns
    assert 'value' in df.columns
    assert df['value'].iloc[0] == 3.5


@patch('requests.get')
def test_load_data_with_friendly_name(mock_get):
    """Test loading data using friendly name."""
    mock_response = Mock()
    mock_response.json.return_value = {
        'observations': [{'date': '2020-01-01', 'value': '3.5'}]
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    loader = FREDLoader(api_key='test_key')
    df = loader.load('unemployment_rate')  # Friendly name
    
    # Should translate to UNRATE
    call_args = mock_get.call_args
    assert call_args[1]['params']['series_id'] == 'UNRATE'


@patch('requests.get')
def test_load_data_api_error(mock_get):
    """Test API error handling."""
    mock_response = Mock()
    mock_response.json.return_value = {
        'error_code': 400,
        'error_message': 'Invalid series ID'
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    loader = FREDLoader(api_key='test_key')
    with pytest.raises(DataLoadError, match="Error loading FRED data"):
        loader.load('INVALID')


@patch('requests.get')
def test_load_data_empty_response(mock_get):
    """Test handling of empty API response."""
    mock_response = Mock()
    mock_response.json.return_value = {'observations': []}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    loader = FREDLoader(api_key='test_key')
    with pytest.raises(DataLoadError, match="No data returned"):
        loader.load('TEST')


def test_filter_dates():
    """Test date filtering."""
    loader = FREDLoader(api_key='test_key')
    
    df = pd.DataFrame({
        'date': pd.date_range('2020-01-01', periods=12, freq='ME'),
        'value': range(12)
    })
    
    # Filter start date
    filtered = loader._filter_dates(df, '2020-03-01', None)
    assert len(filtered) == 10
    assert filtered['date'].min() >= pd.Timestamp('2020-03-01')
    
    # Filter end date
    filtered = loader._filter_dates(df, None, '2020-06-30')
    assert len(filtered) == 6
    assert filtered['date'].max() <= pd.Timestamp('2020-06-30')
    
    # Filter both
    filtered = loader._filter_dates(df, '2020-03-01', '2020-06-30')
    assert len(filtered) == 4


def test_convenience_methods():
    """Test convenience methods."""
    with patch.object(FREDLoader, 'load') as mock_load:
        mock_load.return_value = pd.DataFrame({'date': [], 'value': []})
        
        loader = FREDLoader(api_key='test_key')
        
        loader.load_treasury_spread()
        mock_load.assert_called_with('treasury_spread')
        
        loader.load_unemployment_rate()
        mock_load.assert_called_with('unemployment_rate')
        
        loader.load_mortgage_rates()
        mock_load.assert_called_with('mortgage_rates')