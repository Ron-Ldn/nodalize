import os
from abc import ABC
from datetime import date
from unittest import TestCase

import dask.dataframe as dd
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.compute as pc

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.custom_dependencies.lag import LagDependency
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
from nodalize.data_management.duckdb_data_manager import DuckdbDataManager
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator
from nodalize.tools.dates import unix_now
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
    def calculator_type(self) -> str:
        return "pandas"

    def compute(self, parameters):
        df = pd.DataFrame()

        if parameters.get("mode") == "update":
            df["StockId"] = [2, 3]
            df["Price"] = [5.5, 6.6]
        else:
            if parameters["DataDate"] == date(2022, 1, 1):
                df["StockId"] = [i for i in range(5)]
                df["Price"] = [i * 1.0 for i in range(5)]
            elif parameters["DataDate"] == date(2022, 1, 2):
                df["StockId"] = [i for i in range(4)]
                df["Price"] = [i + i * i * 0.1 for i in range(4)]
            else:
                raise NotImplementedError(
                    f"Unexpected data date: {parameters['DataDate']}"
                )

        return df


class EquityPriceMove(EquityStock):
    @property
    def schema(self):
        return {
            "PriceMove": (float, ColumnCategory.VALUE),
        }

    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self) -> str:
        return self._calc

    @property
    def dependencies(self):
        return {
            "lagged_price": LagDependency(
                "EquityPrice",
                1,
                data_fields={
                    "StockId": "StockId",
                    "Price": "PrevPrice",
                    "DataDate": "DataDate",
                },
            ),
            "equity_price": "EquityPrice",
        }

    def compute(self, parameters, lagged_price, equity_price):
        lagged_price_df = lagged_price(ignore_deltas=True)
        equity_price_df = equity_price()

        if parameters.get("mode") == "update":
            how = "inner"
        else:
            how = "outer"

        if self.calculator.calc_type == "pandas":
            lagged_price_df[column_names.DATA_DATE] = lagged_price_df[
                column_names.DATA_DATE
            ].astype(equity_price_df[column_names.DATA_DATE].dtype)

            df = pd.merge(
                lagged_price_df,
                equity_price_df,
                how,
                on=["StockId", "DataDate"],
                suffixes=("", "_right"),
            )
        elif self.calculator.calc_type == "dask":
            lagged_price_df[column_names.DATA_DATE] = lagged_price_df[
                column_names.DATA_DATE
            ].astype(equity_price_df[column_names.DATA_DATE].dtype)

            df = dd.merge(
                lagged_price_df,
                equity_price_df,
                how,
                on=["StockId", "DataDate"],
                suffixes=("", "_right"),
            )
        elif self.calculator.calc_type == "polars":
            lagged_price_df = self.calculator.add_column(
                lagged_price_df,
                column_names.DATA_DATE,
                pl.col(column_names.DATA_DATE).cast(
                    equity_price_df[column_names.DATA_DATE].dtype
                ),
            )

            df = lagged_price_df.join(
                equity_price_df, how=how, on=["StockId", "DataDate"]
            )
        elif self.calculator.calc_type == "pyarrow":
            if how == "inner":
                join_type = "inner"
            elif how == "outer":
                join_type = "full outer"
            else:
                raise NotImplementedError

            lagged_price_df = self.calculator.add_column(
                lagged_price_df,
                column_names.DATA_DATE,
                pc.cast(
                    lagged_price_df[column_names.DATA_DATE],
                    equity_price_df[column_names.DATA_DATE].type,
                ),
            )
            df = lagged_price_df.join(
                equity_price_df, keys=["StockId", "DataDate"], join_type=join_type
            )
        elif self.calculator.calc_type == "spark":
            df = lagged_price_df.join(
                equity_price_df, how=how, on=["StockId", "DataDate"]
            )
        else:
            raise NotImplementedError

        return self.calculator.add_column(
            df,
            "PriceMove",
            (
                self.calculator.get_column(df, "Price")
                - self.calculator.get_column(df, "PrevPrice")
            )
            / self.calculator.get_column(df, "PrevPrice"),
        )


class TestColumnarWithLag(TestCase):
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

        coordinator.create_data_node(EquityPrice)
        node = coordinator.create_data_node(EquityPriceMove)
        node.set_calculator_type(calc_type)
        coordinator.set_up()

        # Compute first date
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 1)},
        )

        # Check data
        df = calculator.to_pandas(node.load(calculator=calculator))
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]
        expected_df["DataDate"] = [
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 1),
            date(2022, 1, 2),
        ]
        expected_df["PriceMove"] = [np.NaN for i in range(10)]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

        # Compute second date
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 2)},
        )

        expected_df = pd.DataFrame()
        expected_df["StockId"] = [0, 1, 2, 3, 4]
        expected_df["DataDate"] = [
            date(2022, 1, 2),
            date(2022, 1, 2),
            date(2022, 1, 2),
            date(2022, 1, 2),
            date(2022, 1, 2),
        ]
        expected_df["PriceMove"] = [
            np.NaN,
            0.1,
            0.2,
            0.3,
            np.NaN,
        ]

        df = calculator.to_pandas(
            node.load(
                calculator=calculator, filters=[[("DataDate", "=", date(2022, 1, 2))]]
            )
        )
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId", "DataDate"]),
        )

        # Delta update
        now = unix_now()

        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 2), "mode": "update"},
        )

        expected_df = pd.DataFrame()
        expected_df["StockId"] = [2, 3]
        expected_df["DataDate"] = [
            date(2022, 1, 2),
            date(2022, 1, 2),
        ]
        expected_df["PriceMove"] = [
            1.75,
            1.2,
        ]

        df = calculator.to_pandas(
            node.load(
                calculator=calculator,
                filters=[
                    [
                        ("DataDate", "=", date(2022, 1, 2)),
                        ("InsertedDatetime", ">", now),
                    ]
                ],
            )
        )
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date

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

    # @use_temp_folder
    # def test_pyarrow_sqlite(self):
    #     self.run_test(
    #         "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
    #     )

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

    @use_temp_folder
    def test_pandas_duckdb(self):
        self.run_test(
            "pandas", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_duckdb(self):
        self.run_test(
            "dask", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_duckdb(self):
        self.run_test(
            "spark", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_duckdb(self):
        self.run_test(
            "pyarrow", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_duckdb(self):
        self.run_test(
            "polars", DuckdbDataManager(os.path.join(self.temp_directory, "test.db"))
        )
