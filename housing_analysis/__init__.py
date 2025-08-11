"""Housing Analysis Package - A modular package for housing market data analysis."""

__version__ = "0.1.0"

# Legacy import for backward compatibility
from .datasets import Datasets

# New modular imports
from .data import DataManager, FREDLoader, NYFedLoader
from .config import config
from . import utils
from . import visualization
from . import exceptions

__all__ = [
    # Legacy
    "Datasets",
    # New modules
    "DataManager",
    "FREDLoader",
    "NYFedLoader",
    "config",
    "utils",
    "visualization",
    "exceptions",
]