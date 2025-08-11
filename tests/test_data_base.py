"""Tests for base data loader."""

import pytest
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
from housing_analysis.data.base import DataLoader


class ConcreteLoader(DataLoader):
    """Concrete implementation for testing."""
    
    def load(self, **kwargs):
        return pd.DataFrame({'date': pd.date_range('2020-01-01', periods=5),
                            'value': range(5)})
    
    def get_available_series(self):
        return ['test_series']


def test_data_loader_initialization():
    """Test DataLoader initialization."""
    loader = ConcreteLoader()
    assert loader.cache_dir is None
    
    with TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir) / 'cache'
        loader = ConcreteLoader(cache_dir=cache_dir)
        assert loader.cache_dir == cache_dir
        assert cache_dir.exists()


def test_cache_operations():
    """Test cache save and load operations."""
    with TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir)
        loader = ConcreteLoader(cache_dir=cache_dir)
        
        # Create test data
        df = pd.DataFrame({'date': pd.date_range('2020-01-01', periods=5),
                          'value': range(5)})
        
        # Test save to cache
        loader._save_to_cache(df, 'test_id')
        cache_file = cache_dir / 'test_id.parquet'
        assert cache_file.exists()
        
        # Test load from cache
        loaded_df = loader._load_from_cache('test_id')
        assert loaded_df is not None
        pd.testing.assert_frame_equal(df, loaded_df)
        
        # Test non-existent cache
        missing_df = loader._load_from_cache('missing_id')
        assert missing_df is None


def test_cache_path_generation():
    """Test cache path generation."""
    with TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir)
        loader = ConcreteLoader(cache_dir=cache_dir)
        
        path = loader._get_cache_path('test_id')
        assert path == cache_dir / 'test_id.parquet'
        
    # Test without cache_dir
    loader = ConcreteLoader()
    with pytest.raises(ValueError, match="Cache directory not set"):
        loader._get_cache_path('test_id')


def test_abstract_methods():
    """Test that abstract methods must be implemented."""
    with pytest.raises(TypeError):
        DataLoader()