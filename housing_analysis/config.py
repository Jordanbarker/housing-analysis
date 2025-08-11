"""Configuration management for housing analysis."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Centralized configuration for housing analysis."""
    
    # API Keys
    FRED_API_KEY: Optional[str] = os.getenv("FRED_API_KEY")
    
    # Paths
    BASE_DIR: Path = Path(__file__).parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)
    
    # FRED API Configuration
    FRED_BASE_URL: str = "https://api.stlouisfed.org/fred/series/observations"
    
    # NY Fed Data URLs
    NY_FED_MORTGAGE_URL: str = (
        "https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/"
        "xls/hhdc_mortgage_debt_balance_and_delinquency_status.xlsx"
    )
    
    # Census Data Configuration
    CENSUS_API_BASE: str = "https://api.census.gov/data"
    
    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.FRED_API_KEY:
            raise ValueError("FRED_API_KEY environment variable not set.")


config = Config()