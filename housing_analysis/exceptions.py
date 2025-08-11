"""Custom exceptions for housing analysis."""


class HousingAnalysisError(Exception):
    """Base exception for housing analysis."""
    pass


class DataLoadError(HousingAnalysisError):
    """Exception raised when data loading fails."""
    pass


class APIError(HousingAnalysisError):
    """Exception raised for API-related errors."""
    pass


class ConfigurationError(HousingAnalysisError):
    """Exception raised for configuration issues."""
    pass


class DataNotFoundError(HousingAnalysisError):
    """Exception raised when requested data is not found."""
    pass