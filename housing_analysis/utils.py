"""General utilities for housing analysis."""

from typing import Optional
import pandas as pd
import numpy as np


def calculate_year_over_year_change(data: pd.DataFrame, 
                                   value_column: str = 'value',
                                   date_column: str = 'date') -> pd.Series:
    """Calculate year-over-year percentage change.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data with date and value columns.
    value_column : str
        Name of value column.
    date_column : str
        Name of date column.
        
    Returns
    -------
    pd.Series
        Year-over-year percentage change.
    """
    # Ensure data is sorted by date
    data = data.sort_values(date_column)
    
    # Calculate 12-month percentage change
    return data[value_column].pct_change(periods=12) * 100


def calculate_mortgage_payment(principal: float, 
                              rate: float, 
                              years: int = 30) -> float:
    """Calculate monthly mortgage payment.
    
    Parameters
    ----------
    principal : float
        Loan principal amount.
    rate : float
        Annual interest rate (as percentage).
    years : int
        Loan term in years.
        
    Returns
    -------
    float
        Monthly payment amount.
    """
    monthly_rate = rate / 100 / 12
    n_payments = years * 12
    
    if monthly_rate == 0:
        return principal / n_payments
    
    payment = principal * (monthly_rate * (1 + monthly_rate)**n_payments) / \
              ((1 + monthly_rate)**n_payments - 1)
    
    return payment


def calculate_affordability_ratio(house_price: float,
                                 income: float,
                                 down_payment_pct: float = 0.2) -> float:
    """Calculate house price to income ratio.
    
    Parameters
    ----------
    house_price : float
        House price.
    income : float
        Annual household income.
    down_payment_pct : float
        Down payment as percentage of house price.
        
    Returns
    -------
    float
        Price to income ratio.
    """
    return house_price / income


def resample_to_quarterly(data: pd.DataFrame,
                         date_column: str = 'date',
                         value_column: str = 'value',
                         method: str = 'mean') -> pd.DataFrame:
    """Resample data to quarterly frequency.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to resample.
    date_column : str
        Name of date column.
    value_column : str
        Name of value column.
    method : str
        Aggregation method ('mean', 'sum', 'last').
        
    Returns
    -------
    pd.DataFrame
        Quarterly resampled data.
    """
    data = data.set_index(date_column)
    
    if method == 'mean':
        resampled = data[value_column].resample('QE').mean()
    elif method == 'sum':
        resampled = data[value_column].resample('QE').sum()
    elif method == 'last':
        resampled = data[value_column].resample('QE').last()
    else:
        raise ValueError(f"Unknown method: {method}")
    
    return resampled.reset_index()


def normalize_series(data: pd.DataFrame,
                    value_column: str = 'value',
                    base_date: Optional[str] = None) -> pd.Series:
    """Normalize series to base value of 100.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to normalize.
    value_column : str
        Name of value column.
    base_date : Optional[str]
        Date to use as base (100). If None, uses first date.
        
    Returns
    -------
    pd.Series
        Normalized series.
    """
    if base_date:
        base_value = data[data['date'] == pd.to_datetime(base_date)][value_column].iloc[0]
    else:
        base_value = data[value_column].iloc[0]
    
    return (data[value_column] / base_value) * 100


def calculate_moving_average(data: pd.DataFrame,
                            value_column: str = 'value',
                            window: int = 12) -> pd.Series:
    """Calculate moving average.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to smooth.
    value_column : str
        Name of value column.
    window : int
        Window size for moving average.
        
    Returns
    -------
    pd.Series
        Moving average series.
    """
    return data[value_column].rolling(window=window, min_periods=1).mean()