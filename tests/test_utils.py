"""Tests for utility functions."""

import pytest
import pandas as pd
import numpy as np
from housing_analysis.utils import (
    calculate_year_over_year_change,
    calculate_mortgage_payment,
    calculate_affordability_ratio,
    resample_to_quarterly,
    normalize_series,
    calculate_moving_average
)


def test_calculate_year_over_year_change():
    """Test year-over-year change calculation."""
    dates = pd.date_range('2020-01-01', periods=24, freq='ME')
    values = list(range(100, 124))
    df = pd.DataFrame({'date': dates, 'value': values})
    
    yoy = calculate_year_over_year_change(df)
    
    # First 12 months should be NaN
    assert yoy[:12].isna().all()
    # After 12 months, should have values
    assert not yoy[12:].isna().any()
    # Check a specific calculation
    assert yoy.iloc[12] == pytest.approx(12.0)  # (112-100)/100 * 100


def test_calculate_mortgage_payment():
    """Test mortgage payment calculation."""
    # Test with 0% interest
    payment = calculate_mortgage_payment(300000, 0, 30)
    assert payment == pytest.approx(300000 / 360)
    
    # Test with typical values
    payment = calculate_mortgage_payment(300000, 4.5, 30)
    assert payment == pytest.approx(1520.06, abs=0.01)
    
    # Test with different term
    payment = calculate_mortgage_payment(300000, 4.5, 15)
    assert payment == pytest.approx(2294.98, abs=0.01)


def test_calculate_affordability_ratio():
    """Test affordability ratio calculation."""
    ratio = calculate_affordability_ratio(400000, 100000)
    assert ratio == 4.0
    
    ratio = calculate_affordability_ratio(400000, 100000, 0.1)
    assert ratio == 4.0  # Down payment doesn't affect ratio


def test_resample_to_quarterly():
    """Test quarterly resampling."""
    dates = pd.date_range('2020-01-01', periods=12, freq='ME')
    values = list(range(1, 13))
    df = pd.DataFrame({'date': dates, 'value': values})
    
    # Test mean resampling
    quarterly = resample_to_quarterly(df, method='mean')
    assert len(quarterly) == 4
    assert quarterly['value'].iloc[0] == pytest.approx(2.0)  # Mean of 1, 2, 3
    
    # Test sum resampling
    quarterly = resample_to_quarterly(df, method='sum')
    assert quarterly['value'].iloc[0] == 6  # Sum of 1, 2, 3
    
    # Test last resampling
    quarterly = resample_to_quarterly(df, method='last')
    assert quarterly['value'].iloc[0] == 3  # Last value of Q1


def test_normalize_series():
    """Test series normalization."""
    dates = pd.date_range('2020-01-01', periods=5, freq='ME')
    values = [100, 110, 120, 130, 140]
    df = pd.DataFrame({'date': dates, 'value': values})
    
    # Test normalization to first date
    normalized = normalize_series(df)
    assert normalized.iloc[0] == 100.0
    assert normalized.iloc[1] == pytest.approx(110.0)
    assert normalized.iloc[-1] == pytest.approx(140.0)
    
    # Test normalization to specific date
    normalized = normalize_series(df, base_date='2020-03-31')
    assert normalized.iloc[0] == pytest.approx(83.33, abs=0.01)
    assert normalized.iloc[2] == 100.0


def test_calculate_moving_average():
    """Test moving average calculation."""
    values = list(range(1, 13))
    df = pd.DataFrame({'value': values})
    
    # Test 3-period moving average
    ma = calculate_moving_average(df, window=3)
    assert len(ma) == 12
    assert ma.iloc[2] == pytest.approx(2.0)  # (1+2+3)/3
    assert ma.iloc[3] == pytest.approx(3.0)  # (2+3+4)/3