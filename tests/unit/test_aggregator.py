"""Unit tests for aggregator functions."""
import pytest
from datetime import datetime, date
import pandas as pd
from src.data_marts.aggregator import (
    normalize_client_name,
    get_clients_info,
    aggregate_client_sales,
    aggregate_client_trainings
)


class TestNormalizeClientName:
    """Tests for normalize_client_name function."""
    
    def test_basic_normalization(self):
        assert normalize_client_name("Иванов Иван") == "ИвановИван"
        
    def test_removes_whitespace(self):
        assert normalize_client_name("Иванов   Иван  Иванович") == "ИвановИванИванович"
        
    def test_removes_tabs_newlines(self):
        assert normalize_client_name("Иванов\tИван\nИванович") == "ИвановИванИванович"
        
    def test_handles_none(self):
        assert normalize_client_name(None) == ""
        
    def test_handles_empty_string(self):
        assert normalize_client_name("") == ""
        
    def test_handles_nan(self):
        assert normalize_client_name(pd.NA) == ""
    
    def test_case_preservation(self):
        """Normalization preserves case."""
        assert normalize_client_name("ABC xyz") == "ABCxyz"


# Note: calculate_age and calculate_birthday_message are nested functions
# inside calculate_client_balance, so they cannot be imported directly.
# They should be tested via integration tests or refactored to be top-level.


class TestAggregatorIntegration:
    """Integration tests for aggregator functions."""
    
    @pytest.mark.skip(reason="Requires database connection")
    def test_get_clients_info_structure(self):
        """Test that get_clients_info returns expected columns."""
        # This would require a test database or mock
        pass
    
    @pytest.mark.skip(reason="Requires database connection")
    def test_aggregate_client_sales_structure(self):
        """Test that aggregate_client_sales returns expected structure."""
        pass
    
    @pytest.mark.skip(reason="Requires database connection")
    def test_aggregate_client_trainings_structure(self):
        """Test that aggregate_client_trainings returns expected structure."""
        pass

