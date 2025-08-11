"""Tests for custom exceptions."""

import pytest
from housing_analysis.exceptions import (
    HousingAnalysisError,
    DataLoadError,
    APIError,
    ConfigurationError,
    DataNotFoundError
)


def test_base_exception():
    """Test base exception."""
    with pytest.raises(HousingAnalysisError):
        raise HousingAnalysisError("Test error")


def test_data_load_error():
    """Test data load error."""
    with pytest.raises(DataLoadError) as exc_info:
        raise DataLoadError("Failed to load data")
    assert "Failed to load data" in str(exc_info.value)


def test_api_error():
    """Test API error."""
    with pytest.raises(APIError) as exc_info:
        raise APIError("API request failed")
    assert "API request failed" in str(exc_info.value)


def test_configuration_error():
    """Test configuration error."""
    with pytest.raises(ConfigurationError) as exc_info:
        raise ConfigurationError("Invalid configuration")
    assert "Invalid configuration" in str(exc_info.value)


def test_data_not_found_error():
    """Test data not found error."""
    with pytest.raises(DataNotFoundError) as exc_info:
        raise DataNotFoundError("Data not found")
    assert "Data not found" in str(exc_info.value)


def test_exception_inheritance():
    """Test that all exceptions inherit from base."""
    assert issubclass(DataLoadError, HousingAnalysisError)
    assert issubclass(APIError, HousingAnalysisError)
    assert issubclass(ConfigurationError, HousingAnalysisError)
    assert issubclass(DataNotFoundError, HousingAnalysisError)