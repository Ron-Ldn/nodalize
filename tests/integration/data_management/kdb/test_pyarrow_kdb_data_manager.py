from unittest import TestCase

from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from tests.integration.data_management.base_test_data_manager import BaseTestDataManager
from tests.integration.data_management.kdb.base_test_kdb_data_manager import (
    BaseTestKdbDataManager,
)


class TestIntegrationPyarrowKdbDataManager(
    TestCase, BaseTestKdbDataManager, BaseTestDataManager
):
    def setUp(self):
        self.clean_up()

    def tearDown(self):
        self.clean_up()

    @property
    def calculator_type(self):
        return PyarrowCalculator

    @property
    def data_manager(self):
        return self.create_kdb_data_manager()

    def test_save_and_load_no_partitioning(self):
        self._test_save_and_load(False)

    def test_save_and_load_with_partitioning(self):
        self._test_save_and_load(True)

    def test_save_and_load_and_filter_no_partitioning(self):
        self._test_save_and_load_and_filter(False)

    def test_save_and_load_and_filter_with_partitioning(self):
        self._test_save_and_load_and_filter(True)

    def test_save_and_load_complex_filter_no_partitioning(self):
        self._test_save_and_load_complex_filter(False)

    def test_save_and_load_complex_filter_with_partitioning(self):
        self._test_save_and_load_complex_filter(True)

    def test_symbol_filters(self):
        self._test_symbol_filters()

    def test_string_filters(self):
        self._test_string_filters()

    def test_symbol_filters_with_spaces(self):
        self._test_symbol_filters_with_spaces()

    def test_string_filters_with_spaces(self):
        self._test_string_filters_with_spaces()

    def test_float_filters(self):
        self._test_float_filters()

    def test_int_filters(self):
        self._test_int_filters()

    def test_date_filters(self):
        self._test_date_filters()

    def test_datetime_filters(self):
        self._test_datetime_filters()

    def test_bool_filters(self):
        self._test_bool_filters()

    def test_load_data_frame_with_individual_lookback(self):
        self._test_load_data_frame_with_individual_lookback()

    def test_load_data_frame_with_group_lookback(self):
        self._test_load_data_frame_with_group_lookback()
