"""Data module for housing analysis."""

from .fred import FREDLoader
from .newyorkfed import NYFedLoader
from .census import CensusLoader
from .loaders import DataManager

__all__ = [
    'FREDLoader',
    'NYFedLoader', 
    'CensusLoader',
    'DataManager',
]