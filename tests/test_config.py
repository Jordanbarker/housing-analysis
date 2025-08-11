"""Tests for configuration module."""

import pytest
from pathlib import Path
from unittest.mock import patch
import os
from housing_analysis.config import Config


def test_config_with_api_key():
    """Test config with API key set."""
    config = Config()
    # Test that config can access environment variable
    assert config.FRED_API_KEY is not None or os.getenv('FRED_API_KEY') is not None
    # Test validate with a key
    old_key = config.FRED_API_KEY
    config.FRED_API_KEY = 'test_key'
    config.validate()  # Should not raise
    config.FRED_API_KEY = old_key


def test_config_validate_missing_key():
    """Test config validation with missing API key."""
    # Save original key
    original_key = Config.FRED_API_KEY
    # Force it to be None
    Config.FRED_API_KEY = None
    with pytest.raises(ValueError, match="FRED_API_KEY"):
        Config.validate()
    # Restore original key
    Config.FRED_API_KEY = original_key


def test_config_paths():
    """Test configuration paths."""
    config = Config()
    assert isinstance(config.BASE_DIR, Path)
    assert isinstance(config.DATA_DIR, Path)
    assert config.DATA_DIR.exists()


def test_config_urls():
    """Test configuration URLs."""
    config = Config()
    assert config.FRED_BASE_URL.startswith("https://")
    assert config.NY_FED_MORTGAGE_URL.startswith("https://")
    assert config.CENSUS_API_BASE.startswith("https://")