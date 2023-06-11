import os
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
from nodalize.tools.static_func_tools import generate_random_name
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


class EquityPrice(DataNode):
    def __init__(self, group_lookback=False, no_key=False, **kwargs):
        DataNode.__init__(self, **kwargs)
        self._group_lookback = group_lookback
        self._no_key = no_key

    @property
    def schema(self):
        if self._no_key:
            return {
                "StockId": (int, ColumnCategory.VALUE),
                "Price": (float, ColumnCategory.VALUE),
            }
        else:
            return {
                "StockId": (int, ColumnCategory.KEY),
                "Price": (float, ColumnCategory.VALUE),
            }

    @property
    def lookback(self):
        return 1

    @property
    def group_lookback(self):
        return self._group_lookback

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        dt1 = date(2022, 1, 1)
        dt2 = date(2022, 1, 2)
        dt3 = date(2022, 1, 3)

        if parameters.get("mode") == "update":
            df = pd.DataFrame()
            df["StockId"] = [4, 1, 1, 1]
            df["DataDate"] = [dt1, dt1, dt2, dt3]
            df["Price"] = [4.2, 1.1, 1.2, 1.3]
            return df
        else:
            df = pd.DataFrame()
            df["StockId"] = [1, 2, 3, 4, 1, 2, 1, 3]
            df["DataDate"] = [dt1, dt1, dt1, dt1, dt2, dt2, dt3, dt3]
            df["Price"] = [0.1, 2.1, 3.1, 4.1, 0.2, 2.2, 0.3, 3.3]
            return df


class EquityPriceLookback1(DataNode):
    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self):
        return self._calc

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"EquityPrice": None}


class EquityPriceLookback2(DataNode):
    def set_calculator_type(self, calc):
        self._calc = calc

    @property
    def calculator_type(self):
        return self._calc

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"EquityPrice": DateDependency("EquityPrice", lookback=2)}


class TestLookback(TestCase):
    def setUp(self):
        self.temp_directory = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "temp" + generate_random_name(4)
        )
        self.s3_bucket = s3_bucket
        self.s3_folder = "temp" + generate_random_name(4)

    def run_test_individual_lookback(self, calc_type, data_manager, **kwargs):
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
        coordinator.create_data_node(EquityPriceLookback1).set_calculator_type(
            calc_type
        )
        coordinator.create_data_node(EquityPriceLookback2).set_calculator_type(
            calc_type
        )
        coordinator.set_up()

        # Compute
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 3)},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback1")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 2, 3]
        expected_df["DataDate"] = [date(2022, 1, 3), date(2022, 1, 3), date(2022, 1, 3)]
        expected_df["Price"] = [0.3, 2.2, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 2, 3, 4]
        expected_df["DataDate"] = [
            date(2022, 1, 3),
            date(2022, 1, 3),
            date(2022, 1, 3),
            date(2022, 1, 3),
        ]
        expected_df["Price"] = [0.3, 2.2, 3.3, 4.1]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        # Update
        now = unix_now()

        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 3), "mode": "update"},
        )

        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        df = df.loc[df["InsertedDatetime"] > now].sort_values(
            by=["StockId", "DataDate"]
        )

        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 4]
        expected_df["DataDate"] = [
            date(2022, 1, 3),
            date(2022, 1, 3),
        ]
        expected_df["Price"] = [1.3, 4.2]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date

        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 2, 3, 4]
        expected_df["DataDate"] = [
            date(2022, 1, 3),
            date(2022, 1, 3),
            date(2022, 1, 3),
            date(2022, 1, 3),
        ]
        expected_df["Price"] = [1.3, 2.2, 3.3, 4.2]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

    def run_test_group_lookback(self, calc_type, data_manager, **kwargs):
        # Set-up environment
        coordinator = Coordinator("test")
        coordinator.set_data_manager("default", data_manager, default=True)
        coordinator.set_calculator(calc_type, **kwargs)
        calculator = coordinator.get_calculator(calc_type)

        def load_from_table_as_pandas(id):
            node = coordinator._cache.get_data_node(id)
            df = node.load(calculator=calculator)
            return calculator.to_pandas(df)

        coordinator.create_data_node(EquityPrice, group_lookback=True)
        coordinator.create_data_node(EquityPriceLookback2).set_calculator_type(
            calc_type
        )
        coordinator.set_up()

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4)},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [0.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4), "mode": "update"},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [1.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

    def run_test_no_key(self, calc_type, data_manager, **kwargs):
        # Set-up environment
        coordinator = Coordinator("test")
        coordinator.set_data_manager("default", data_manager, default=True)
        coordinator.set_calculator(calc_type, **kwargs)
        calculator = coordinator.get_calculator(calc_type)

        def load_from_table_as_pandas(id):
            node = coordinator._cache.get_data_node(id)
            df = node.load(calculator=calculator)
            return calculator.to_pandas(df)

        coordinator.create_data_node(EquityPrice, no_key=True)
        coordinator.create_data_node(EquityPriceLookback2).set_calculator_type(
            calc_type
        )
        coordinator.set_up()

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4)},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [0.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4), "mode": "update"},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [1.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

    def run_test_no_key_and_group_lookback(self, calc_type, data_manager, **kwargs):
        # Set-up environment
        coordinator = Coordinator("test")
        coordinator.set_data_manager("default", data_manager, default=True)
        coordinator.set_calculator(calc_type, **kwargs)
        calculator = coordinator.get_calculator(calc_type)

        def load_from_table_as_pandas(id):
            node = coordinator._cache.get_data_node(id)
            df = node.load(calculator=calculator)
            return calculator.to_pandas(df)

        coordinator.create_data_node(EquityPrice, group_lookback=True, no_key=True)
        coordinator.create_data_node(EquityPriceLookback2).set_calculator_type(
            calc_type
        )
        coordinator.set_up()

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4)},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [0.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

        # Compute EquityPrice over 3 days
        coordinator.run_recursively(
            node_identifiers=["EquityPrice"],
            global_parameters={"DataDate": date(2022, 1, 4), "mode": "update"},
        )

        # Check
        df = load_from_table_as_pandas("EquityPriceLookback2")
        df[column_names.DATA_DATE] = pd.to_datetime(df[column_names.DATA_DATE]).dt.date
        expected_df = pd.DataFrame()
        expected_df["StockId"] = [1, 3]
        expected_df["DataDate"] = [date(2022, 1, 4), date(2022, 1, 4)]
        expected_df["Price"] = [1.3, 3.3]

        compare_data_frames(
            expected_df,
            df.drop(columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID])
            .round(3)
            .sort_values(by=["StockId"]),
        )

    @use_temp_folder
    def test_pandas_individual_lookback_file(self):
        self.run_test_individual_lookback(
            "pandas", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_individual_lookback_file(self):
        self.run_test_individual_lookback(
            "dask", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_spark_individual_lookback_file(self):
        self.run_test_individual_lookback(
            "spark", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pyarrow_individual_lookback_file(self):
        self.run_test_individual_lookback(
            "pyarrow", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_individual_lookback_file(self):
        self.run_test_individual_lookback(
            "polars", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pandas_group_lookback_file(self):
        self.run_test_group_lookback(
            "pandas", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_group_lookback_file(self):
        self.run_test_group_lookback("dask", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_group_lookback_file(self):
        self.run_test_group_lookback("spark", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_pyarrow_group_lookback_file(self):
        self.run_test_group_lookback(
            "pyarrow", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_group_lookback_file(self):
        self.run_test_group_lookback(
            "polars", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pandas_no_key_file(self):
        self.run_test_no_key("pandas", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_dask_no_key_file(self):
        self.run_test_no_key("dask", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_no_key_file(self):
        self.run_test_no_key("spark", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_pyarrow_no_key_file(self):
        self.run_test_no_key("pyarrow", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_no_key_file(self):
        self.run_test_no_key("polars", LocalFileDataManager(self.temp_directory))

    @use_temp_folder
    def test_pandas_no_key_and_group_lookback_file(self):
        self.run_test_no_key_and_group_lookback(
            "pandas", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_no_key_and_group_lookback_file(self):
        self.run_test_no_key_and_group_lookback(
            "dask", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_spark_no_key_and_group_lookback_file(self):
        self.run_test_no_key_and_group_lookback(
            "spark", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pyarrow_no_key_and_group_lookback_file(self):
        self.run_test_no_key_and_group_lookback(
            "pyarrow", LocalFileDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_no_key_and_group_lookback_file(self):
        self.run_test_no_key_and_group_lookback(
            "polars", LocalFileDataManager(self.temp_directory)
        )

    def test_pandas_individual_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_individual_lookback(
                "pandas", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_dask_individual_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_individual_lookback(
                "dask", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_spark_individual_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_individual_lookback(
                "spark", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pyarrow_individual_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_individual_lookback(
                "pyarrow", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_polars_individual_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_individual_lookback(
                "polars", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pandas_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_group_lookback(
                "pandas", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_dask_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_group_lookback(
                "dask", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_spark_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_group_lookback(
                "spark", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pyarrow_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_group_lookback(
                "pyarrow", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_polars_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_group_lookback(
                "polars", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pandas_no_key_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key(
                "pandas", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_dask_no_key_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key(
                "dask", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    @use_temp_folder
    def test_spark_no_key_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key(
                "spark", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pyarrow_no_key_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key(
                "pyarrow", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_polars_no_key_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key(
                "polars", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pandas_no_key_and_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key_and_group_lookback(
                "pandas", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_dask_no_key_and_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key_and_group_lookback(
                "dask", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_spark_no_key_and_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key_and_group_lookback(
                "spark", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_pyarrow_no_key_and_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key_and_group_lookback(
                "pyarrow", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    def test_polars_no_key_and_group_lookback_kdb(self):
        try:
            BaseTestKdbDataManager.clean_up()
            self.run_test_no_key_and_group_lookback(
                "polars", BaseTestKdbDataManager.create_kdb_data_manager()
            )
        finally:
            BaseTestKdbDataManager.clean_up()

    @use_temp_folder
    def test_pandas_individual_lookback_delta_lake(self):
        self.run_test_individual_lookback(
            "pandas", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_individual_lookback_delta_lake(self):
        self.run_test_individual_lookback(
            "dask", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_spark_individual_lookback_delta_lake(self):
        self.run_test_individual_lookback(
            "spark",
            DeltaLakeDataManager(self.temp_directory),
            spark_config=build_spark_config(use_delta_lake=True),
        )

    @use_temp_folder
    def test_pyarrow_individual_lookback_delta_lake(self):
        self.run_test_individual_lookback(
            "pyarrow", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_individual_lookback_delta_lake(self):
        self.run_test_individual_lookback(
            "polars", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pandas_group_lookback_delta_lake(self):
        self.run_test_group_lookback(
            "pandas", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_group_lookback_delta_lake(self):
        self.run_test_group_lookback("dask", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_group_lookback_delta_lake(self):
        self.run_test_group_lookback(
            "spark",
            DeltaLakeDataManager(self.temp_directory),
            spark_config=build_spark_config(use_delta_lake=True),
        )

    @use_temp_folder
    def test_pyarrow_group_lookback_delta_lake(self):
        self.run_test_group_lookback(
            "pyarrow", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_group_lookback_delta_lake(self):
        self.run_test_group_lookback(
            "polars", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pandas_no_key_delta_lake(self):
        self.run_test_no_key("pandas", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_dask_no_key_delta_lake(self):
        self.run_test_no_key("dask", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_spark_no_key_delta_lake(self):
        self.run_test_no_key("spark", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_pyarrow_no_key_delta_lake(self):
        self.run_test_no_key("pyarrow", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_polars_no_key_delta_lake(self):
        self.run_test_no_key("polars", DeltaLakeDataManager(self.temp_directory))

    @use_temp_folder
    def test_pandas_no_key_and_group_lookback_delta_lake(self):
        self.run_test_no_key_and_group_lookback(
            "pandas", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_dask_no_key_and_group_lookback_delta_lake(self):
        self.run_test_no_key_and_group_lookback(
            "dask", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_spark_no_key_and_group_lookback_delta_lake(self):
        self.run_test_no_key_and_group_lookback(
            "spark", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pyarrow_no_key_and_group_lookback_delta_lake(self):
        self.run_test_no_key_and_group_lookback(
            "pyarrow", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_polars_no_key_and_group_lookback_delta_lake(self):
        self.run_test_no_key_and_group_lookback(
            "polars", DeltaLakeDataManager(self.temp_directory)
        )

    @use_temp_folder
    def test_pandas_individual_lookback_sqlite(self):
        self.run_test_individual_lookback(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_individual_lookback_sqlite(self):
        self.run_test_individual_lookback(
            "dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_individual_lookback_sqlite(self):
        self.run_test_individual_lookback(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_individual_lookback_sqlite(self):
        self.run_test_individual_lookback(
            "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_individual_lookback_sqlite(self):
        self.run_test_individual_lookback(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pandas_group_lookback_sqlite(self):
        self.run_test_group_lookback(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_group_lookback_sqlite(self):
        self.run_test_group_lookback(
            "dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_group_lookback_sqlite(self):
        self.run_test_group_lookback(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_group_lookback_sqlite(self):
        self.run_test_group_lookback(
            "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_group_lookback_sqlite(self):
        self.run_test_group_lookback(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pandas_no_key_sqlite(self):
        self.run_test_no_key(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_no_key_sqlite(self):
        self.run_test_no_key(
            "dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_no_key_sqlite(self):
        self.run_test_no_key(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_no_key_sqlite(self):
        self.run_test_no_key(
            "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_no_key_sqlite(self):
        self.run_test_no_key(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pandas_no_key_and_group_lookback_sqlite(self):
        self.run_test_no_key_and_group_lookback(
            "pandas", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_dask_no_key_and_group_lookback_sqlite(self):
        self.run_test_no_key_and_group_lookback(
            "dask", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_spark_no_key_and_group_lookback_sqlite(self):
        self.run_test_no_key_and_group_lookback(
            "spark", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_pyarrow_no_key_and_group_lookback_sqlite(self):
        self.run_test_no_key_and_group_lookback(
            "pyarrow", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_folder
    def test_polars_no_key_and_group_lookback_sqlite(self):
        self.run_test_no_key_and_group_lookback(
            "polars", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )

    @use_temp_s3_folder
    def test_pandas_individual_lookback_s3(self):
        self.run_test_individual_lookback(
            "pandas", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_dask_individual_lookback_s3(self):
        self.run_test_individual_lookback(
            "dask", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_spark_individual_lookback_s3(self):
        self.run_test_individual_lookback(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    @use_temp_s3_folder
    def test_pyarrow_individual_lookback_s3(self):
        self.run_test_individual_lookback(
            "pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_polars_individual_lookback_s3(self):
        self.run_test_individual_lookback(
            "polars", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_pandas_group_lookback_s3(self):
        self.run_test_group_lookback(
            "pandas", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_dask_group_lookback_s3(self):
        self.run_test_group_lookback(
            "dask", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_spark_group_lookback_s3(self):
        self.run_test_group_lookback(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    @use_temp_s3_folder
    def test_pyarrow_group_lookback_s3(self):
        self.run_test_group_lookback(
            "pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_s3_folder
    def test_polars_group_lookback_s3(self):
        self.run_test_group_lookback(
            "polars", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_pandas_no_key_s3(self):
        self.run_test_no_key(
            "pandas", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_dask_no_key_s3(self):
        self.run_test_no_key("dask", S3FileDataManager(self.s3_bucket, self.s3_folder))

    @use_temp_folder
    def test_spark_no_key_s3(self):
        self.run_test_no_key(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    @use_temp_folder
    def test_pyarrow_no_key_s3(self):
        self.run_test_no_key(
            "pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_polars_no_key_s3(self):
        self.run_test_no_key(
            "polars", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_pandas_no_key_and_group_lookback_s3e(self):
        self.run_test_no_key_and_group_lookback(
            "pandas", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_dask_no_key_and_group_lookback_s3(self):
        self.run_test_no_key_and_group_lookback(
            "dask", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_spark_no_key_and_group_lookback_s3(self):
        self.run_test_no_key_and_group_lookback(
            "spark",
            S3FileDataManager(self.s3_bucket, self.s3_folder),
            spark_config=build_spark_config(use_s3=True),
        )

    @use_temp_folder
    def test_pyarrow_no_key_and_group_lookback_s3(self):
        self.run_test_no_key_and_group_lookback(
            "pyarrow", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )

    @use_temp_folder
    def test_polars_no_key_and_group_lookback_s3(self):
        self.run_test_no_key_and_group_lookback(
            "polars", S3FileDataManager(self.s3_bucket, self.s3_folder)
        )
