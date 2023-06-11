import os
from datetime import date
from unittest import TestCase

import pandas as pd

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator
from tests.common import (
    build_spark_config,
    compare_data_frames,
    s3_bucket,
    use_temp_folder,
    use_temp_s3_folder,
)
from tests.integration.data_management.kdb.base_test_kdb_data_manager import (
    BaseTestKdbDataManager,
)


class BaseDataSet(DataNode):
    @property
    def schema(self):
        return {
            "Id": (int, ColumnCategory.KEY),
            "Numerator": (float, ColumnCategory.VALUE),
            "Denominator": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        return pd.DataFrame(
            {
                "Id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "Numerator": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0],
                "Denominator": [10, 5, 30, 0.25, 25, 3, 0.7, 4, 3],
            }
        )


class EnrichedDataSet(DataNode):
    def set_calculator_type(self, calc_type):
        """Set calculation framework."""
        self.calc_type = calc_type

    @property
    def schema(self):
        """Define storage format."""
        return {
            "Id": (int, ColumnCategory.KEY),
            "Ratio": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        """Define parent nodes."""
        return {"base_data": "BaseDataSet"}

    @property
    def calculator_type(self):
        """Define type of calculation framework."""
        return self.calc_type

    def compute(self, parameters, base_data):
        """Compute data."""
        df = base_data()
        df = self.calculator.add_column(
            df,
            "Ratio",
            self.calculator.get_column(df, "Numerator")
            / self.calculator.get_column(df, "Denominator"),
        )
        return df


class TestBasicDemo(TestCase):
    def setUp(self):
        self.temp_directory = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "temp"
        )
        self.s3_bucket = s3_bucket
        self.s3_folder = "temp"

    def run_test(self, calc_type, data_manager, **calculator_kwargs):
        # Set-up environment
        column_names.DATA_DATE = "Date"
        coordinator = Coordinator("test")
        coordinator.set_data_manager("default", data_manager, default=True)
        calculator = coordinator.set_calculator(calc_type, **calculator_kwargs)

        coordinator.create_data_node(BaseDataSet)
        enriched_data_set_node = coordinator.create_data_node(EnrichedDataSet)
        enriched_data_set_node.set_calculator_type(calc_type)
        coordinator.set_up()

        # Compute
        coordinator.run_recursively(
            node_identifiers=["BaseDataSet"],
            global_parameters={column_names.DATA_DATE: date(2022, 1, 2)},
        )

        # Reload saved data
        saved_data = enriched_data_set_node.load()
        pandas_data = calculator.to_pandas(saved_data)
        pandas_data[column_names.DATA_DATE] = pd.to_datetime(
            pandas_data[column_names.DATA_DATE]
        ).dt.date

        # Check data
        expected_data = pd.DataFrame(
            {
                "Id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "Ratio": [1, 4, 1, 160, 2, 20, 100, 20, 30],
                column_names.DATA_DATE: [date(2022, 1, 2)] * 9,
            }
        )

        compare_data_frames(
            expected_data,
            pandas_data.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            ).sort_values(by=["Id"]),
        )

    @use_temp_folder
    def test_pandas_file(self):
        self.run_test("pandas", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_dask_file(self):
        self.run_test("dask", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_file(self):
        self.run_test("spark", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_pyarrow_file(self):
        self.run_test("pyarrow", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_file(self):
        self.run_test("polars", LocalFileDataManager(self.temp_directory))

    def test_pandas_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("pandas", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_dask_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("dask", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_spark_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("spark", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pyarrow_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("pyarrow", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_polars_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("polars", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    @use_temp_folder
    def test_pandas_delta_lake(self):
        self.run_test("pandas", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_dask_delta_lake(self):
        self.run_test("dask", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_delta_lake(self):
        self.run_test(
            "spark",
            DeltaLakeDataManager(self.temp_directory),
            spark_config=build_spark_config(use_delta_lake=True),
        )

    @use_temp_folder
    def test_pyarrow_delta_lake(self):
        self.run_test("pyarrow", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_delta_lake(self):
        self.run_test("polars", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_pandas_sqlite(self):
        self.run_test(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_sqlite(self):
        self.run_test(
            "dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_sqlite(self):
        self.run_test(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_sqlite(self):
        self.run_test(
            "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_sqlite(self):
        self.run_test(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_s3_folder
    def test_pandas_s3(self):
        self.run_test("pandas", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_s3_folder
    def test_dask_s3(self):
        self.run_test("dask", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_s3_folder
    def test_spark_s3(self):
        self.run_test(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    @use_temp_s3_folder
    def test_pyarrow_s3(self):
        self.run_test("pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_s3_folder
    def test_polars_s3(self):
        self.run_test("polars", S3FileDataManager(self.s3_bucket, self.s3_folder))
