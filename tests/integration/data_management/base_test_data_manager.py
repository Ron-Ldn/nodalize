from abc import ABC
from datetime import date, datetime, timedelta

import pandas as pd

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.tools.dates import unix_now
from tests.common import compare_data_frames


class BaseTestDataManager(ABC):
    @property
    def calculator_type(self):
        raise NotImplementedError

    @property
    def data_manager(self):
        raise NotImplementedError

    def _test_save_and_load(self, with_partitioning):
        calculator = self.calculator_type("test")
        loader = self.data_manager
        partitions = ["Id"] if with_partitioning else None
        table_name = "mytable"

        schema = {
            "Id": (int, ColumnCategory.KEY),
            "DataDate": (datetime, ColumnCategory.PARAMETER),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
            datetime(2022, 1, 3),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3]
        pandas_df[column_names.INSERTED_DATETIME] = unix_now()

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        loader.save_updates(calculator, table_name, schema, partitions, 1234, target_df)

        reloaded_df = loader.load_data_frame(calculator, table_name, schema)

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(pandas_df, ret_df.sort_values(["Id", "DataDate"]))

        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3, 3]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
            datetime(2022, 1, 3),
            datetime(2022, 1, 4),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3, 103.4]
        pandas_df[column_names.INSERTED_DATETIME] = unix_now()
        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        loader.save_updates(calculator, table_name, schema, partitions, 5678, target_df)

        reloaded_df = loader.load_data_frame(calculator, table_name, schema)

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(pandas_df, ret_df.sort_values(["Id", "DataDate"]))

    def _test_save_and_load_and_filter(self, with_partitioning):
        calculator = self.calculator_type("test")
        loader = self.data_manager
        partitions = ["Id1"] if with_partitioning else None
        table_name = "mytable"

        schema = {
            "Id1": (int, ColumnCategory.KEY),
            "Id2": (int, ColumnCategory.VALUE),
            "DataDate": (datetime, ColumnCategory.PARAMETER),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2, 2, 3]
        pandas_df["Id2"] = [4, 5, 6, 7]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
            datetime(2022, 1, 3),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3, 103.4]
        now = unix_now()
        pandas_df[column_names.INSERTED_DATETIME] = now

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        loader.save_updates(calculator, table_name, schema, partitions, 1334, target_df)

        # 2 filters:
        # Id1 != 1
        # DataDate < 2022-01-03
        column_to_select = ["Id1", "DataDate", "Price"]
        filters = [[("Id1", "!=", 1), ("DataDate", "<", datetime(2022, 1, 3))]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            columns=column_to_select,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [2, 2]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
        ]
        pandas_df["Price"] = [101.2, 102.3]

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # 2 filters:
        # Id1 == 2
        # DataDate > 2022-01-01
        filters = [[("Id1", "=", 2), ("DataDate", ">", datetime(2022, 1, 1))]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [2]
        pandas_df["Id2"] = [6]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 2),
        ]
        pandas_df["Price"] = [102.3]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # 2 filters:
        # Id1 >= 2
        # DataDate <= 2022-01-02
        filters = [[("Id1", ">=", 2), ("DataDate", "<=", datetime(2022, 1, 2))]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [2, 2]
        pandas_df["Id2"] = [5, 6]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
        ]
        pandas_df["Price"] = [101.2, 102.3]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # 2 filters:
        # Id1 in [1, 2]
        # DataDate not in [2022-01-02, 2022-01-03]
        filters = [
            [
                ("Id1", "in", [1, 2]),
                ("DataDate", "not in", [datetime(2022, 1, 2), datetime(2022, 1, 3)]),
            ]
        ]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2]
        pandas_df["Id2"] = [4, 5]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
        ]
        pandas_df["Price"] = [100.1, 101.2]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # "Or" filter
        # Id1 != 1 and DataDate < 2022-01-03
        # Or Id1 == 1
        filters = [
            [("Id1", "!=", 1), ("DataDate", "<", datetime(2022, 1, 3))],
            [("Id1", "=", 1)],
        ]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2, 2]
        pandas_df["Id2"] = [4, 5, 6]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # "Or" filter
        # Id1 != 1 and DataDate < 2022-01-02
        # Or Id1 in [1, 3]
        filters = [
            [("Id1", "!=", 1), ("DataDate", "<", datetime(2022, 1, 2))],
            [("Id1", "in", [1, 3])],
        ]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2, 3]
        pandas_df["Id2"] = [4, 5, 7]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 3),
        ]
        pandas_df["Price"] = [100.1, 101.2, 103.4]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

        # "Or" filter
        # Id1 != 1 and DataDate < 2022-01-02
        # Or Id1 not in [2, 3]
        filters = [
            [("Id1", "!=", 1), ("DataDate", "<", datetime(2022, 1, 2))],
            [("Id1", "not in", [2, 3])],
        ]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2]
        pandas_df["Id2"] = [4, 5]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
        ]
        pandas_df["Price"] = [100.1, 101.2]
        pandas_df[column_names.INSERTED_DATETIME] = now

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1", "DataDate"]),
        )

    def _test_save_and_load_complex_filter(self, with_partitioning):
        now1 = unix_now()
        calculator = self.calculator_type("test")
        loader = self.data_manager
        table_name = "mytable"
        partitions = ["Id1"] if with_partitioning else None

        schema = {
            "Id1": (int, ColumnCategory.KEY),
            "Id2": (str, ColumnCategory.VALUE),
            "DataDate": (datetime, ColumnCategory.PARAMETER),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (Symbol, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2, 3, 4, 1, 2, 3]
        pandas_df["Id2"] = ["AA", "BB", "CC", "DD", "AA", "BB", "CC"]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3, 103.4, 200.1, 201.2, 202.3]
        pandas_df["Label"] = ["A", "B", "C", "A", "A", "B", "C"]
        now2 = unix_now()
        pandas_df[column_names.INSERTED_DATETIME] = [
            now1,
            now1,
            now1,
            now1,
            now2,
            now2,
            now2,
        ]

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        loader.save_updates(calculator, table_name, schema, partitions, 1234, target_df)

        filters = [[("Price", "<", 201)]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 4]
        pandas_df["Id2"] = ["AA", "DD"]
        pandas_df["DataDate"] = [datetime(2022, 1, 1), datetime(2022, 1, 1)]
        pandas_df["Price"] = [200.1, 103.4]
        pandas_df["Label"] = ["A", "A"]
        pandas_df[column_names.INSERTED_DATETIME] = [now2, now1]

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1"]),
        )

        filters = [[("Price", "<", 202), ("Id1", "=", 1)], [("Id2", "=", "CC")]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 3]
        pandas_df["Id2"] = ["AA", "CC"]
        pandas_df["DataDate"] = [datetime(2022, 1, 1), datetime(2022, 1, 1)]
        pandas_df["Price"] = [200.1, 202.3]
        pandas_df["Label"] = ["A", "C"]
        pandas_df[column_names.INSERTED_DATETIME] = now2

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1"]),
        )

        filters = [[("Label", "=", "A")]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 4]
        pandas_df["Id2"] = ["AA", "DD"]
        pandas_df["DataDate"] = [datetime(2022, 1, 1), datetime(2022, 1, 1)]
        pandas_df["Price"] = [200.1, 103.4]
        pandas_df["Label"] = ["A", "A"]
        pandas_df[column_names.INSERTED_DATETIME] = [now2, now1]

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1"]),
        )

        filters = [[("Id1", ">", 1), ("Price", "<", 203)], [("Id2", "=", "BB")]]
        reloaded_df = loader.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=filters,
        )

        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [2, 3, 4]
        pandas_df["Id2"] = ["BB", "CC", "DD"]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
            datetime(2022, 1, 1),
        ]
        pandas_df["Price"] = [201.2, 202.3, 103.4]
        pandas_df["Label"] = ["B", "C", "A"]
        pandas_df[column_names.INSERTED_DATETIME] = [now2, now2, now1]

        ret_df = calculator.to_pandas(reloaded_df)
        ret_df["Price"] = ret_df["Price"].round(4)

        compare_data_frames(
            pandas_df,
            ret_df.sort_values(["Id1"]),
        )

    def _test_symbol_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "=", "ABC0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "!=", "ABC0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(1, row_num)],
                "Price": [1.1 * i for i in range(1, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "in", ["ABC0", "ABC1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "Price": [0.0, 1.1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "not in", ["ABC0", "ABC1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(2, row_num)],
                "Price": [1.1 * i for i in range(2, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_symbol_filters_with_spaces(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "=", "ABC 0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC 0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "!=", "ABC 0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(1, row_num)],
                "Price": [1.1 * i for i in range(1, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "in", ["ABC 0", "ABC 1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC 0", "ABC 1"],
                "Price": [0.0, 1.1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "not in", ["ABC 0", "ABC 1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(2, row_num)],
                "Price": [1.1 * i for i in range(2, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_string_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (str, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )

        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "=", "ABC0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "!=", "ABC0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(1, row_num)],
                "Price": [1.1 * i for i in range(1, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "in", ["ABC0", "ABC1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "Price": [0.0, 1.1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "not in", ["ABC0", "ABC1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(2, row_num)],
                "Price": [1.1 * i for i in range(2, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_string_filters_with_spaces(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (str, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )

        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "=", "ABC 0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC 0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "!=", "ABC 0")]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(1, row_num)],
                "Price": [1.1 * i for i in range(1, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "in", ["ABC 0", "ABC 1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC 0", "ABC 1"],
                "Price": [0.0, 1.1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Security", "not in", ["ABC 0", "ABC 1"])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC {i}" for i in range(2, row_num)],
                "Price": [1.1 * i for i in range(2, row_num)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_float_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )

        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "=", 0.0)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<=", 1.1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "Price": [0.0, 1.1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 1.1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0.0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "!=", 1.1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i != 1],
                "Price": [1.1 * i for i in range(row_num) if i != 1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", ">=", 8.8)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "Price": [8.8, 9.9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", ">", 8.8)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC9"],
                "Price": [9.9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "in", [1.1, 8.8])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC1", "ABC8"],
                "Price": [1.1, 8.8],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "not in", [i * 1.1 for i in range(8)])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "Price": [8.8, 9.9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_int_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (int, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Price": [1 * i for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "=", 0)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<=", 1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "Price": [0, 1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "Price": [0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "!=", 1)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i != 1],
                "Price": [1 * i for i in range(row_num) if i != 1],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", ">=", 8)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "Price": [8, 9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", ">", 8)]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC9"],
                "Price": [9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "in", [1, 8])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC1", "ABC8"],
                "Price": [1, 8],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "not in", [i for i in range(8)])]],
            columns=["Security", "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "Price": [8, 9],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_date_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "DataDate": (date, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "DataDate": [date(2023, 1, 1) + timedelta(i) for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "=", date(2023, 1, 1))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "DataDate": [datetime(2023, 1, 1)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "<=", date(2023, 1, 2))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "DataDate": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "<", date(2023, 1, 2))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "DataDate": [datetime(2023, 1, 1)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "!=", date(2023, 1, 2))]],
            columns=["DataDate", "Security"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i != 1],
                "DataDate": [
                    datetime(2023, 1, 1) + timedelta(i)
                    for i in range(row_num)
                    if i != 1
                ],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", ">=", date(2023, 1, 9))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "DataDate": [datetime(2023, 1, 9), datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", ">", date(2023, 1, 9))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC9"],
                "DataDate": [datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "in", [date(2023, 1, 1), date(2023, 1, 9)])]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC8"],
                "DataDate": [datetime(2023, 1, 1), datetime(2023, 1, 9)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[
                [
                    (
                        "DataDate",
                        "not in",
                        [date(2023, 1, 1) + timedelta(i) for i in range(8)],
                    )
                ]
            ],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "DataDate": [datetime(2023, 1, 9), datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_datetime_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "DataDate": (datetime, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "DataDate": [
                    datetime(2023, 1, 1) + timedelta(i) for i in range(row_num)
                ],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "=", datetime(2023, 1, 1))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "DataDate": [datetime(2023, 1, 1)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "<=", datetime(2023, 1, 2))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1"],
                "DataDate": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "<", datetime(2023, 1, 2))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0"],
                "DataDate": [datetime(2023, 1, 1)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", "!=", datetime(2023, 1, 2))]],
            columns=["DataDate", "Security"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i != 1],
                "DataDate": [
                    datetime(2023, 1, 1) + timedelta(i)
                    for i in range(row_num)
                    if i != 1
                ],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", ">=", datetime(2023, 1, 9))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "DataDate": [datetime(2023, 1, 9), datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("DataDate", ">", datetime(2023, 1, 9))]],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC9"],
                "DataDate": [datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[
                [("DataDate", "in", [datetime(2023, 1, 1), datetime(2023, 1, 9)])]
            ],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC8"],
                "DataDate": [datetime(2023, 1, 1), datetime(2023, 1, 9)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[
                [
                    (
                        "DataDate",
                        "not in",
                        [datetime(2023, 1, 1) + timedelta(i) for i in range(8)],
                    )
                ]
            ],
            columns=["Security", "DataDate"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC8", "ABC9"],
                "DataDate": [datetime(2023, 1, 9), datetime(2023, 1, 10)],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_bool_filters(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.VALUE),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
        }

        row_num = 10
        pandas_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": [i % 2 == 0 for i in range(row_num)],
                column_names.INSERTED_DATETIME: unix_now(),
            }
        )
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Clean", "=", True)]],
            columns=["Security", "Clean"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i % 2 == 0],
                "Clean": [True for i in range(row_num) if i % 2 == 0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Clean", "!=", False)]],
            columns=["Security", "Clean"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i % 2 == 0],
                "Clean": [True for i in range(row_num) if i % 2 == 0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Clean", "in", [True])]],
            columns=["Security", "Clean"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i % 2 == 0],
                "Clean": [True for i in range(row_num) if i % 2 == 0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

        reloaded_df = self.data_manager.load_data_frame(
            calculator,
            table_name,
            schema,
            filters=[[("Clean", "not in", [False])]],
            columns=["Security", "Clean"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num) if i % 2 == 0],
                "Clean": [True for i in range(row_num) if i % 2 == 0],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_load_data_frame_with_individual_lookback(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
        }

        dt1 = date(2022, 1, 1)
        dt2 = date(2022, 1, 2)
        dt3 = date(2022, 1, 3)
        now = unix_now()
        pandas_df = pd.DataFrame(
            {
                "Security": ["1", "2", "3", "4", "1", "2", "1", "3"],
                column_names.DATA_DATE: [dt1, dt1, dt1, dt1, dt2, dt2, dt3, dt3],
                "Price": [1.1, 2.1, 3.1, 4.1, 1.2, 2.2, 1.3, 3.3],
                column_names.INSERTED_DATETIME: [now] * 8,
                column_names.BATCH_ID: [1234] * 8,
            }
        )

        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame_with_lookback(
            calculator,
            table_name,
            schema,
            dt3,
            1,
            False,
            columns=["Security", column_names.DATA_DATE, "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["1", "2", "3"],
                column_names.DATA_DATE: [dt3, dt3, dt3],
                "Price": [1.3, 2.2, 3.3],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )

    def _test_load_data_frame_with_group_lookback(self):
        table_name = "mytesttable"
        calculator = self.calculator_type("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
        }

        dt1 = date(2022, 1, 1)
        dt2 = date(2022, 1, 2)
        dt3 = date(2022, 1, 3)
        dt4 = date(2022, 1, 4)
        now = unix_now()
        pandas_df = pd.DataFrame(
            {
                "Security": ["1", "2", "3", "4", "1", "2", "1", "3"],
                column_names.DATA_DATE: [dt1, dt1, dt1, dt1, dt2, dt2, dt3, dt3],
                "Price": [1.1, 2.1, 3.1, 4.1, 1.2, 2.2, 1.3, 3.3],
                column_names.INSERTED_DATETIME: [now] * 8,
                column_names.BATCH_ID: [1234] * 8,
            }
        )

        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        self.data_manager.save_updates(calculator, table_name, schema, None, 1234, df)

        reloaded_df = self.data_manager.load_data_frame_with_lookback(
            calculator,
            table_name,
            schema,
            dt4,
            2,
            True,
            columns=["Security", column_names.DATA_DATE, "Price"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["1", "3"],
                column_names.DATA_DATE: [dt4, dt4],
                "Price": [1.3, 3.3],
            }
        )

        compare_data_frames(
            expected_df, calculator.to_pandas(reloaded_df).sort_values(by="Security")
        )
