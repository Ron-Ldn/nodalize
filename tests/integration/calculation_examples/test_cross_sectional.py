import os
from abc import ABC
from datetime import date
from unittest import TestCase

import numpy as np
import pandas as pd
import polars as pl
from pyspark.sql import functions as spark_f
from pyspark.sql.window import Window

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
from nodalize.data_management.duckdb_data_manager import DuckdbDataManager
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition
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


class EquityStock(DataNode, ABC):
    def get_enriched_schema(self):
        baseSchema = DataNode.get_enriched_schema(self)
        baseSchema["StockId"] = (int, ColumnCategory.KEY)
        return baseSchema


class EquityPrice(EquityStock):
    @property
    def schema(self):
        return {
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        df = pd.DataFrame()

        if parameters.get("mode") == "update":
            df["StockId"] = [i for i in range(1, 4)]
            df["Price"] = [i * 2.2 for i in range(1, 4)]
        else:
            df["StockId"] = [i for i in range(5)]
            df["Price"] = [i * 1.1 for i in range(5)]
        return df


class EquityRank(EquityStock):
    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self):
        return self._calc

    @property
    def schema(self):
        return {
            "Rank": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"equity_price": DependencyDefinition("EquityPrice")}

    def compute(self, parameters, equity_price):
        df = equity_price(ignore_deltas=True)

        if self._calc == "pandas":
            df["Rank"] = np.trunc(df["Price"].rank())
            return df
        elif self._calc == "spark":
            return df.withColumn(
                "Rank",
                spark_f.rank()
                .over(Window.partitionBy("DataDate").orderBy("Price"))
                .cast("float"),
            )
        elif self._calc == "polars":
            return df.with_columns(
                pl.col("Price").rank().cast(pl.Int64).cast(pl.Float64).alias("Rank")
            )
        else:
            raise NotImplementedError


class TestCrossSectional(TestCase):
    def setUp(self):
        self.temp_directory = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "temp"
        )
        self.s3_bucket = s3_bucket
        self.s3_folder = "temp"

    def run_test(self, calc_type, data_manager, **kwargs):
        # Set-up environment
        coordinator = Coordinator("test")
        coordinator.set_data_manager("default", data_manager, default=True)
        coordinator.set_calculator(calc_type, **kwargs)
        calculator = coordinator.get_calculator(calc_type)

        def load_from_table_as_pandas(id):
            node = coordinator._cache.get_data_node(id)
            df = node.load(calculator=calculator)
            return calculator.to_pandas(df)

        coordinator.create_data_node(EquityPrice)
        coordinator.create_data_node(EquityRank).set_calculator_type(calc_type)
        coordinator.set_up()

        # Compute all
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 1)},
        )

        # Check data
        df = load_from_table_as_pandas("EquityRank")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [0, 1, 2, 3, 4]
        expected_df["DataDate"] = [date(2022, 1, 1) for _ in range(5)]
        expected_df["Rank"] = [1, 2, 3, 4, 5]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

        # Compute update
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 1), "mode": "update"},
        )

        df = load_from_table_as_pandas("EquityRank")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [0, 1, 2, 3, 4]
        expected_df["DataDate"] = [date(2022, 1, 1) for _ in range(5)]
        expected_df["Rank"] = [1, 2, 3, 5, 3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

    @use_temp_folder
    def test_pandas_file(self):
        self.run_test("pandas", LocalFileDataManager(self.temp_directory))

    # Dask does not support rank
    # @use_temp_folder
    # def test_dask_file(self):
    #     self.run_test("dask", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_file(self):
        self.run_test("spark", LocalFileDataManager(self.temp_directory))

    # Can we compute ranks with PyArrow?
    # @use_temp_folder
    # def test_pyarrow_file(self):
    #     self.run_test("pyarrow", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_file(self):
        self.run_test("polars", LocalFileDataManager(self.temp_directory))

    def test_pandas_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("pandas", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    # Dask does not support rank
    # def test_dask_kdb(self):
    #     try:
    #         BaseTestKdbDataManager.clean_up()
    #         self.run_test("dask", BaseTestKdbDataManager.create_kdb_data_manager())
    #     finally:
    #         BaseTestKdbDataManager.clean_up()

    def test_spark_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("spark", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    # Can we compute ranks with PyArrow?
    # def test_pyarrow_kdb(self):
    #     try:
    #         BaseTestKdbDataManager.clean_up()
    #         self.run_test("pyarrow", BaseTestKdbDataManager.create_kdb_data_manager())
    #     finally:
    #         BaseTestKdbDataManager.clean_up()

    def test_polars_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test("polars", BaseTestKdbDataManager.create_kdb_data_manager())
        finally:
            BaseTestKdbDataManager.clean_up()

    @use_temp_folder
    def test_pandas_delta_lake(self):
        self.run_test("pandas", DeltaLakeDataManager(self.temp_directory))

    # Dask does not support rank
    # @use_temp_folder
    # def test_dask_delta_lake(self):
    #     self.run_test("dask", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_delta_lake(self):
        self.run_test(
            "spark",
            DeltaLakeDataManager(self.temp_directory),
            spark_config=build_spark_config(use_delta_lake=True),
        )

    # Can we compute ranks with PyArrow?
    # @use_temp_folder
    # def test_pyarrow_delta_lake(self):
    #     self.run_test("pyarrow", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_delta_lake(self):
        self.run_test("polars", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_pandas_sqlite(self):
        self.run_test(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    # Dask does not support rank
    # @use_temp_folder
    # def test_dask_sqlite(self):
    #     self.run_test("dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db")))

    @use_temp_folder
    def test_spark_sqlite(self):
        self.run_test(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    # Can we compute ranks with PyArrow?
    # @use_temp_folder
    # def test_pyarrow_sqlite(self):
    #     self.run_test("pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db")))

    @use_temp_folder
    def test_polars_sqlite(self):
        self.run_test(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_s3_folder
    def test_pandas_s3(self):
        self.run_test("pandas", S3FileDataManager(self.s3_bucket, self.s3_folder))

    # Dask does not support rank
    # @use_temp_s3_folder
    # def test_dask_s3(self):
    #     self.run_test("dask", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_s3_folder
    def test_spark_s3(self):
        self.run_test(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    # Can we compute ranks with PyArrow?
    # @use_temp_s3_folder
    # def test_pyarrow_s3(self):
    #     self.run_test("pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_s3_folder
    def test_polars_s3(self):
        self.run_test("polars", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_folder
    def test_pandas_duckdb(self):
        self.run_test(
            "pandas", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    # Dask does not support rank
    # @use_temp_folder
    # def test_dask_duckdb(self):
    #     self.run_test(
    #         "dask", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
    #     )

    @use_temp_folder
    def test_spark_duckdb(self):
        self.run_test(
            "spark", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    # Can we compute ranks with PyArrow?
    # @use_temp_folder
    # def test_pyarrow_duckdb(self):
    #     self.run_test(
    #         "pyarrow", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
    #     )

    @use_temp_folder
    def test_polars_duckdb(self):
        self.run_test(
            "polars", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )
