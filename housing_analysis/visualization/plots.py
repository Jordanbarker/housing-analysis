"""Plotting utilities for housing analysis."""

from typing import Optional, List, Tuple
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.axes import Axes


def plot_recessions(ax: Axes, data: pd.DataFrame, recessions: pd.DataFrame) -> None:
    """Add recession shading to a plot.
    
    Parameters
    ----------
    ax : matplotlib.axes.Axes
        Axes to add recession shading to.
    data : pd.DataFrame
        Data being plotted (must have 'date' column).
    recessions : pd.DataFrame
        Recession data with 'Start' and 'End' columns.
    """
    for _, row in recessions.iterrows():
        if row['End'] > data['date'].min():
            ax.axvspan(row['Start'], row['End'], color='gray', alpha=0.3)


def plot_time_series(data: pd.DataFrame, 
                    column: str = 'value',
                    title: Optional[str] = None,
                    ylabel: Optional[str] = None,
                    recessions: Optional[pd.DataFrame] = None,
                    figsize: Tuple[int, int] = (12, 4)) -> Axes:
    """Plot a time series with optional recession shading.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to plot (must have 'date' column).
    column : str
        Column name to plot.
    title : Optional[str]
        Plot title.
    ylabel : Optional[str]
        Y-axis label.
    recessions : Optional[pd.DataFrame]
        Recession data for shading.
    figsize : Tuple[int, int]
        Figure size.
        
    Returns
    -------
    matplotlib.axes.Axes
        Plot axes.
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    ax.plot(data['date'], data[column])
    
    if recessions is not None:
        plot_recessions(ax, data, recessions)
    
    if title:
        ax.set_title(title)
    if ylabel:
        ax.set_ylabel(ylabel)
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    return ax


def plot_multiple_series(data: pd.DataFrame,
                        columns: List[str],
                        title: Optional[str] = None,
                        ylabel: Optional[str] = None,
                        recessions: Optional[pd.DataFrame] = None,
                        figsize: Tuple[int, int] = (12, 6)) -> Axes:
    """Plot multiple time series on the same axes.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to plot (must have 'date' column).
    columns : List[str]
        Column names to plot.
    title : Optional[str]
        Plot title.
    ylabel : Optional[str]
        Y-axis label.
    recessions : Optional[pd.DataFrame]
        Recession data for shading.
    figsize : Tuple[int, int]
        Figure size.
        
    Returns
    -------
    matplotlib.axes.Axes
        Plot axes.
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    for column in columns:
        ax.plot(data['date'], data[column], label=column)
    
    if recessions is not None:
        plot_recessions(ax, data, recessions)
    
    if title:
        ax.set_title(title)
    if ylabel:
        ax.set_ylabel(ylabel)
    
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    return ax


def plot_spread_with_fill(data: pd.DataFrame,
                          column: str = 'value',
                          title: Optional[str] = None,
                          ylabel: Optional[str] = None,
                          recessions: Optional[pd.DataFrame] = None,
                          zero_line: bool = True,
                          figsize: Tuple[int, int] = (12, 4)) -> Axes:
    """Plot a spread series with filled area below zero.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to plot (must have 'date' column).
    column : str
        Column name to plot.
    title : Optional[str]
        Plot title.
    ylabel : Optional[str]
        Y-axis label.
    recessions : Optional[pd.DataFrame]
        Recession data for shading.
    zero_line : bool
        Whether to add horizontal line at zero.
    figsize : Tuple[int, int]
        Figure size.
        
    Returns
    -------
    matplotlib.axes.Axes
        Plot axes.
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    ax.plot(data['date'], data[column])
    
    # Fill area below zero
    ax.fill_between(data['date'], data[column], 0, 
                    where=data[column] < 0,
                    color='red', alpha=0.3)
    
    if recessions is not None:
        plot_recessions(ax, data, recessions)
    
    if zero_line:
        ax.axhline(y=0, color='gray', linestyle='--')
    
    if title:
        ax.set_title(title)
    if ylabel:
        ax.set_ylabel(ylabel)
    
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    
    return ax