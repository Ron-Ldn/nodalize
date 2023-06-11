import os
from abc import ABC
from datetime import date
from unittest import TestCase

import pandas as pd

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.custom_dependencies.date import DateDependency
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
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


class EquityRawPrice(EquityStock):
    @property
    def schema(self):
        return {
            "Price": (float, ColumnCategory.VALUE),
            "AdjustmentFactor": (float, ColumnCategory.VALUE),
            "Currency": (str, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        if "mode" in parameters and parameters["mode"] == "delta_update":
            df = pd.DataFrame()
            df["StockId"] = [0]
            df["Price"] = [0.1]
            df["AdjustmentFactor"] = [1.0]
            df["Currency"] = ["USD"]
            return df
        else:
            df = pd.DataFrame()
            df["StockId"] = [i for i in range(5)]
            df["Price"] = [i * 1.1 for i in range(5)]
            df["AdjustmentFactor"] = [1.0 if i % 2 == 0 else 0.8 for i in range(5)]
            df["Currency"] = ["USD" if i % 2 == 0 else "GBP" for i in range(5)]
            return df


class FxRate(DataNode):
    @property
    def schema(self):
        return {
            "BaseCurrency": (str, ColumnCategory.KEY),
            "PriceCurrency": (str, ColumnCategory.KEY),
            "Rate": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        df = pd.DataFrame()

        if "mode" in parameters and parameters["mode"] == "delta_update":
            df["BaseCurrency"] = ["GBP"]
            df["PriceCurrency"] = ["USD"]
            df["Rate"] = [0.82]
        else:
            df["BaseCurrency"] = ["USD", "GBP", "EUR"]
            df["PriceCurrency"] = ["USD", "USD", "USD"]
            df["Rate"] = [1.0, 0.84, 0.98]
        return df


class EquityAdjustedPrice(EquityStock):
    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self):
        return self._calc

    @property
    def schema(self):
        return {
            "AdjustedPrice": (float, ColumnCategory.VALUE),
            "AdjustedPriceUSD": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {
            "equity_raw_price": "EquityRawPrice",
            "fx_rate": DateDependency(
                "FxRate",
                data_fields={"Rate": "FxRate", "BaseCurrency": "Currency"},
                filters=[[("PriceCurrency", "=", "USD")]],
            ),
        }

    def compute(self, parameters, equity_raw_price, fx_rate):
        # Note: we have a delta update of both equities and rates. We must reload the rates for all impacted equities,
        # and vice-versa.
        equity_raw_price_df = equity_raw_price(ignore_deltas=True)
        fx_rate_df = fx_rate(ignore_deltas=True)
        df = self.calculator.left_join_data_frames(
            equity_raw_price_df, fx_rate_df, on=["Currency"]
        )
        df = self.calculator.add_column(
            df,
            "AdjustedPrice",
            self.calculator.get_column(df, "Price")
            * self.calculator.get_column(df, "AdjustmentFactor"),
        )
        df = self.calculator.add_column(
            df,
            "AdjustedPriceUSD",
            self.calculator.get_column(df, "AdjustedPrice")
            / self.calculator.get_column(df, "FxRate"),
        )
        return df


class TestColumnarWithJoin(TestCase):
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
        calculator = coordinator.set_calculator(calc_type, **kwargs)

        def load_from_table_as_pandas(id):
            node = coordinator._cache.get_data_node(id)
            df = node.load(calculator=calculator)
            return calculator.to_pandas(df)

        coordinator.create_data_node(EquityRawPrice)
        coordinator.create_data_node(FxRate)
        coordinator.create_data_node(EquityAdjustedPrice).set_calculator_type(calc_type)
        coordinator.set_up()

        # Compute
        coordinator.run_recursively(
            node_identifiers=["EquityRawPrice", "FxRate"],
            global_parameters={"DataDate": date(2022, 1, 2)},
        )

        # Reload saved data
        equity_raw_price_df = load_from_table_as_pandas("EquityRawPrice")
        equity_raw_price_df[column_names.DATA_DATE] = pd.to_datetime(
            equity_raw_price_df[column_names.DATA_DATE]
        ).dt.date
        fx_rate_df = load_from_table_as_pandas("FxRate")
        fx_rate_df[column_names.DATA_DATE] = pd.to_datetime(
            fx_rate_df[column_names.DATA_DATE]
        ).dt.date
        equity_adjusted_price_df = load_from_table_as_pandas("EquityAdjustedPrice")
        equity_adjusted_price_df[column_names.DATA_DATE] = pd.to_datetime(
            equity_adjusted_price_df[column_names.DATA_DATE]
        ).dt.date

        # Check data
        exp_equity_raw_price_df = pd.DataFrame()
        exp_equity_raw_price_df["StockId"] = [0, 1, 2, 3, 4]
        exp_equity_raw_price_df["Price"] = [0, 1.1, 2.2, 3.3, 4.4]
        exp_equity_raw_price_df["AdjustmentFactor"] = [1.0, 0.8, 1.0, 0.8, 1.0]
        exp_equity_raw_price_df["Currency"] = ["USD", "GBP", "USD", "GBP", "USD"]
        exp_equity_raw_price_df["DataDate"] = [date(2022, 1, 2) for i in range(5)]

        compare_data_frames(
            exp_equity_raw_price_df.round(2).sort_values(by=["StockId"]),
            equity_raw_price_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            )
            .round(2)
            .sort_values(by=["StockId"]),
        )

        exp_fx_rate_df = pd.DataFrame()
        exp_fx_rate_df["BaseCurrency"] = ["USD", "GBP", "EUR"]
        exp_fx_rate_df["PriceCurrency"] = ["USD", "USD", "USD"]
        exp_fx_rate_df["Rate"] = [1.0, 0.84, 0.98]
        exp_fx_rate_df["DataDate"] = [date(2022, 1, 2) for i in range(3)]

        compare_data_frames(
            exp_fx_rate_df.round(2).sort_values(by=["BaseCurrency"]),
            fx_rate_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            )
            .round(2)
            .sort_values(by=["BaseCurrency"]),
        )

        exp_equity_adjusted_price_df = pd.DataFrame()
        exp_equity_adjusted_price_df["StockId"] = [0, 1, 2, 3, 4]
        exp_equity_adjusted_price_df["AdjustedPrice"] = [0, 0.88, 2.2, 2.64, 4.4]
        exp_equity_adjusted_price_df["DataDate"] = [date(2022, 1, 2) for i in range(5)]
        exp_equity_adjusted_price_df["AdjustedPriceUSD"] = [0, 1.0476, 2.2, 3.1429, 4.4]
        compare_data_frames(
            exp_equity_adjusted_price_df.round(4).sort_values(by=["StockId"]),
            equity_adjusted_price_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            )
            .round(4)
            .sort_values(by=["StockId"]),
        )

        # Test delta update
        now = unix_now()
        coordinator.run_recursively(
            node_identifiers=["FxRate", "EquityRawPrice"],
            global_parameters={
                "DataDate": date(2022, 1, 2),
                "mode": "delta_update",
            },
        )

        equity_raw_price_df = load_from_table_as_pandas("EquityRawPrice")
        equity_raw_price_df[column_names.DATA_DATE] = pd.to_datetime(
            equity_raw_price_df[column_names.DATA_DATE]
        ).dt.date
        equity_raw_price_df = equity_raw_price_df.loc[
            equity_raw_price_df["InsertedDatetime"] > now
        ]

        exp_equity_raw_price_df = pd.DataFrame()
        exp_equity_raw_price_df["StockId"] = [0]
        exp_equity_raw_price_df["Price"] = [0.1]
        exp_equity_raw_price_df["AdjustmentFactor"] = [1]
        exp_equity_raw_price_df["Currency"] = ["USD"]
        exp_equity_raw_price_df["DataDate"] = [date(2022, 1, 2)]

        compare_data_frames(
            exp_equity_raw_price_df.round(2).sort_values(by=["StockId"]),
            equity_raw_price_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            )
            .round(2)
            .sort_values(by=["StockId"]),
        )

        fx_rate_df = load_from_table_as_pandas("FxRate")
        fx_rate_df[column_names.DATA_DATE] = pd.to_datetime(
            fx_rate_df[column_names.DATA_DATE]
        ).dt.date
        fx_rate_df = fx_rate_df.loc[fx_rate_df["InsertedDatetime"] > now]
        self.assertEquals(1, len(fx_rate_df))

        exp_fx_rate_df = pd.DataFrame()
        exp_fx_rate_df["BaseCurrency"] = ["GBP"]
        exp_fx_rate_df["PriceCurrency"] = ["USD"]
        exp_fx_rate_df["Rate"] = [0.82]
        exp_fx_rate_df["DataDate"] = [date(2022, 1, 2)]

        compare_data_frames(
            exp_fx_rate_df.round(2),
            fx_rate_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            ).round(2),
        )

        equity_adjusted_price_df = load_from_table_as_pandas("EquityAdjustedPrice")
        equity_adjusted_price_df[column_names.DATA_DATE] = pd.to_datetime(
            equity_adjusted_price_df[column_names.DATA_DATE]
        ).dt.date
        equity_adjusted_price_df = equity_adjusted_price_df.loc[
            equity_adjusted_price_df["InsertedDatetime"] > now
        ]
        self.assertEquals(5, len(equity_adjusted_price_df))

        exp_equity_adjusted_price_df = pd.DataFrame()
        exp_equity_adjusted_price_df["StockId"] = [0, 1, 2, 3, 4]
        exp_equity_adjusted_price_df["AdjustedPrice"] = [0.1, 0.88, 2.2, 2.64, 4.4]
        exp_equity_adjusted_price_df["DataDate"] = [date(2022, 1, 2) for i in range(5)]
        exp_equity_adjusted_price_df["AdjustedPriceUSD"] = [
            0.1,
            1.0732,
            2.2,
            3.2195,
            4.4,
        ]
        compare_data_frames(
            exp_equity_adjusted_price_df.round(4).sort_values(by=["StockId"]),
            equity_adjusted_price_df.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            )
            .round(4)
            .sort_values(by=["StockId"]),
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
