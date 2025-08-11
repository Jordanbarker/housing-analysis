import os
import pytest
from dotenv import load_dotenv


def test_fred_api_key_exists():
    """Test that FRED_API_KEY environment variable is set."""
    load_dotenv()
    
    api_key = os.getenv("FRED_API_KEY")
    
    assert api_key is not None, "FRED_API_KEY environment variable not set."
    assert len(api_key) > 0, "FRED_API_KEY is empty."