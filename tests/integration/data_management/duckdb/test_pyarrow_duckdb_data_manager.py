import os
from unittest import TestCase

from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from nodalize.data_management.duckdb_data_manager import DuckdbDataManager
from tests.common import use_temp_folder
from tests.integration.data_management.base_test_data_manager import BaseTestDataManager


class TestIntegrationPyarrowDuckdbDataManager(TestCase, BaseTestDataManager):
    @property
    def calculator_type(self):
        return PyarrowCalculator

    @property
    def temp_directory(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")

    @property
    def data_manager(self):
        return DuckdbDataManager(
            os.path.join(self.temp_directory, "test.db"), schema="testschema"
        )

    @use_temp_folder
    def test_save_and_load_no_partitioning(self):
        self._test_save_and_load(False)

    @use_temp_folder
    def test_save_and_load_with_partitioning(self):
        self._test_save_and_load(True)

    @use_temp_folder
    def test_save_and_load_and_filter_no_partitioning(self):
        self._test_save_and_load_and_filter(False)

    @use_temp_folder
    def test_save_and_load_and_filter_with_partitioning(self):
        self._test_save_and_load_and_filter(True)

    @use_temp_folder
    def test_save_and_load_complex_filter_no_partitioning(self):
        self._test_save_and_load_complex_filter(False)

    @use_temp_folder
    def test_save_and_load_complex_filter_with_partitioning(self):
        self._test_save_and_load_complex_filter(True)

    @use_temp_folder
    def test_symbol_filters(self):
        self._test_symbol_filters()

    @use_temp_folder
    def test_string_filters(self):
        self._test_string_filters()

    @use_temp_folder
    def test_symbol_filters_with_spaces(self):
        self._test_symbol_filters_with_spaces()

    @use_temp_folder
    def test_string_filters_with_spaces(self):
        self._test_string_filters_with_spaces()

    @use_temp_folder
    def test_float_filters(self):
        self._test_float_filters()

    @use_temp_folder
    def test_int_filters(self):
        self._test_int_filters()

    @use_temp_folder
    def test_date_filters(self):
        self._test_date_filters()

    @use_temp_folder
    def test_datetime_filters(self):
        self._test_datetime_filters()

    @use_temp_folder
    def test_bool_filters(self):
        self._test_bool_filters()

    @use_temp_folder
    def test_load_data_frame_with_individual_lookback(self):
        self._test_load_data_frame_with_individual_lookback()

    @use_temp_folder
    def test_load_data_frame_with_group_lookback(self):
        self._test_load_data_frame_with_group_lookback()
