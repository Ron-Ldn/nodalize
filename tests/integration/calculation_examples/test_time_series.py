import os
from abc import ABC
from datetime import date
from unittest import TestCase

import pandas as pd
import polars as pl
import pyarrow as pa
from pyspark.sql import functions as spark_f

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.custom_dependencies.window import WindowDependency
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


class EquityStock(DataNode, ABC):
    def get_enriched_schema(self):
        baseSchema = DataNode.get_enriched_schema(self)
        baseSchema["StockId"] = (int, ColumnCategory.KEY)
        return baseSchema


class EquityPrice(EquityStock):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "Price": (float, ColumnCategory.VALUE),
        }

    def compute(self, parameters):
        df = pd.DataFrame()
        df["StockId"] = [1, 2]

        datadate = parameters["DataDate"]
        df["DataDate"] = [datadate, datadate]

        if datadate == date(2022, 1, 1):
            df["Price"] = [1.1, 2.1]
        elif datadate == date(2022, 1, 2):
            df["Price"] = [1.2, 2.2]
        elif datadate == date(2022, 1, 3):
            df["Price"] = [1.3, 2.3]
        else:
            raise NotImplementedError

        return df


class EquityPriceAverage(EquityStock):
    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self):
        return self._calc

    @property
    def schema(self):
        return {
            "PriceAvg": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"price": WindowDependency("EquityPrice", -2, 0)}

    def compute(self, parameters, price):
        df = price()

        if self._calc in ["pandas", "dask"]:
            df = df[["StockId", "Price"]].groupby(by=["StockId"]).mean()
            df = df.reset_index()
            df = df.rename(columns={"Price": "PriceAvg"})
            return df
        elif self._calc == "spark":
            return (
                df.select(["StockId", "Price"])
                .groupBy("StockId")
                .agg(spark_f.avg("Price").alias("PriceAvg"))
            )
        elif self._calc == "pyarrow":
            df = df.select(["StockId", "Price"])
            df = pa.TableGroupBy(df, "StockId").aggregate([("Price", "mean")])
            names = [
                "StockId" if name == "StockId" else "PriceAvg"
                for name in df.column_names
            ]
            df = df.rename_columns(names)
            return df
        elif self._calc == "polars":
            df = df.select(["StockId", "Price"])
            df = df.groupby(by=["StockId"]).agg(pl.avg("Price").alias("PriceAvg"))
            return df
        else:
            raise NotImplementedError


class TestTimeSeries(TestCase):
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
            df = calculator.to_pandas(df)
            df[column_names.DATA_DATE] = pd.to_datetime(
                df[column_names.DATA_DATE]
            ).dt.date
            return df

        coordinator.create_data_node(EquityPrice)
        coordinator.create_data_node(EquityPriceAverage).set_calculator_type(calc_type)
        coordinator.set_up()

        # First date
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 1)},
        )

        df = load_from_table_as_pandas("EquityPriceAverage")
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 2]
        expected_df["DataDate"] = [date(2022, 1, 1), date(2022, 1, 1)]
        expected_df["PriceAvg"] = [1.1, 2.1]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

        # Second date
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 2)},
        )

        df = load_from_table_as_pandas("EquityPriceAverage")
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 1, 2, 2]
        expected_df["DataDate"] = [
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 1),
            date(2022, 1, 2),
        ]
        expected_df["PriceAvg"] = [1.1, 1.15, 2.1, 2.15]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

        # Third date
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 3)},
        )

        df = load_from_table_as_pandas("EquityPriceAverage")
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 1, 1, 2, 2, 2]
        expected_df["DataDate"] = [
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 3),
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 3),
        ]
        expected_df["PriceAvg"] = [1.1, 1.15, 1.2, 2.1, 2.15, 2.2]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
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
