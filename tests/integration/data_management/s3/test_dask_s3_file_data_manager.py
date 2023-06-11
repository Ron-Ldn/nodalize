from unittest import TestCase

from nodalize.calculators.dask_calculator import DaskCalculator
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
from tests.common import s3_bucket, use_temp_s3_folder
from tests.integration.data_management.base_test_data_manager import BaseTestDataManager


class TestIntegrationDaskS3FileDataManager(TestCase, BaseTestDataManager):
    @property
    def calculator_type(self):
        return DaskCalculator

    @property
    def s3_bucket(self):
        return s3_bucket

    @property
    def s3_folder(self):
        return "temp"

    @property
    def data_manager(self):
        return S3FileDataManager(self.s3_bucket, self.s3_folder)

    @use_temp_s3_folder
    def test_save_and_load_no_partitioning(self):
        self._test_save_and_load(False)

    @use_temp_s3_folder
    def test_save_and_load_with_partitioning(self):
        self._test_save_and_load(True)

    @use_temp_s3_folder
    def test_save_and_load_and_filter_no_partitioning(self):
        self._test_save_and_load_and_filter(False)

    @use_temp_s3_folder
    def test_save_and_load_and_filter_with_partitioning(self):
        self._test_save_and_load_and_filter(True)

    @use_temp_s3_folder
    def test_save_and_load_complex_filter_no_partitioning(self):
        self._test_save_and_load_complex_filter(False)

    @use_temp_s3_folder
    def test_save_and_load_complex_filter_with_partitioning(self):
        self._test_save_and_load_complex_filter(True)

    @use_temp_s3_folder
    def test_symbol_filters(self):
        self._test_symbol_filters()

    @use_temp_s3_folder
    def test_string_filters(self):
        self._test_string_filters()

    @use_temp_s3_folder
    def test_symbol_filters_with_spaces(self):
        self._test_symbol_filters_with_spaces()

    @use_temp_s3_folder
    def test_string_filters_with_spaces(self):
        self._test_string_filters_with_spaces()

    @use_temp_s3_folder
    def test_float_filters(self):
        self._test_float_filters()

    @use_temp_s3_folder
    def test_int_filters(self):
        self._test_int_filters()

    @use_temp_s3_folder
    def test_date_filters(self):
        self._test_date_filters()

    @use_temp_s3_folder
    def test_datetime_filters(self):
        self._test_datetime_filters()

    @use_temp_s3_folder
    def test_bool_filters(self):
        self._test_bool_filters()

    @use_temp_s3_folder
    def test_load_data_frame_with_individual_lookback(self):
        self._test_load_data_frame_with_individual_lookback()

    @use_temp_s3_folder
    def test_load_data_frame_with_group_lookback(self):
        self._test_load_data_frame_with_group_lookback()
