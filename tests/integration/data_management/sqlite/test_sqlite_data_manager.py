import os
from datetime import date, datetime
from unittest import TestCase

import pandas as pd

from nodalize.calculators.pandas_calculator import PandasCalculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.tools.dates import unix_now
from tests.common import compare_data_frames, use_temp_folder


class TestSqliteDataManager(TestCase):
    @property
    def temp_directory(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")

    @use_temp_folder
    def test_check_table_handling(self):
        table_name = "mytesttable"
        sqlite_manager = SqliteDataManager(os.path.join(self.temp_directory, "test.db"))

        self.assertFalse(sqlite_manager.table_exists(table_name))

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
        }

        sqlite_manager.create_table(table_name, schema, None)

        self.assertTrue(sqlite_manager.table_exists(table_name))

        removed = sqlite_manager.check_table_schema(table_name, schema)
        self.assertEqual(0, len(removed))

        new_schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
        }

        removed = sqlite_manager.check_table_schema(table_name, new_schema)
        self.assertEqual({"Label": str}, removed)

        new_schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            "Label2": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
        }

        try:
            sqlite_manager.check_table_schema(table_name, new_schema)
        except AssertionError as e:
            self.assertEqual(
                "Columns in schema are missing in mytesttable: ['Label2']",
                str(e),
            )
        else:
            self.fail("Exception not raised")

        new_schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (int, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
        }

        try:
            sqlite_manager.check_table_schema(table_name, new_schema)
        except AssertionError as e:
            self.assertEqual(
                f"Type mismatch found for column mytesttable.Price: sqlite=REAL whilst schema=INTEGER",
                str(e),
            )
        else:
            self.fail("Exception not raised")

    @use_temp_folder
    def test_check_insert_data(self):
        table_name = "mytesttable"
        sqlite_manager = SqliteDataManager(os.path.join(self.temp_directory, "test.db"))

        calculator = PandasCalculator("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
            column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
        }

        sqlite_manager.create_table(table_name, schema, None)

        row_num = 10
        batch_id1 = 10000
        df1 = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": True,
                "Price": [1.1 * i for i in range(row_num)],
                "Label": [f"ABC{i}" for i in range(row_num)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.INSERTED_DATETIME: unix_now(),
                "InsertedDatetime2": datetime.now().replace(microsecond=0),
                column_names.BATCH_ID: batch_id1,
            }
        )

        sqlite_manager.add_data_to_table(calculator, table_name, schema, {}, df1)

        batch_id2 = 10001
        df2 = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": True,
                "Price": [2.2 * i for i in range(row_num)],
                "Label": [f"ABC{i}" for i in range(row_num)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.INSERTED_DATETIME: unix_now(),
                "InsertedDatetime2": datetime.now().replace(microsecond=0),
                column_names.BATCH_ID: batch_id2,
            }
        )

        sqlite_manager.add_data_to_table(calculator, table_name, schema, {}, df2)

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
        )
        compare_data_frames(df2, reloaded_df)

        row_num2 = 3
        batch_id3 = 10002
        df3 = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num2)],
                "Clean": True,
                "Price": [3.3 * i for i in range(row_num2)],
                "Label": [f"ABC{i}" for i in range(row_num2)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.INSERTED_DATETIME: unix_now(),
                "InsertedDatetime2": datetime.now().replace(microsecond=0),
                column_names.BATCH_ID: batch_id3,
            }
        )

        sqlite_manager.add_data_to_table(calculator, table_name, schema, {}, df3)

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            batch_ids=[batch_id3],
        )
        compare_data_frames(df3, reloaded_df)

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            batch_ids=[batch_id2, batch_id3],
            columns=[
                "Security",
                "Clean",
                "Price",
                "Label",
                column_names.DATA_DATE,
                column_names.BATCH_ID,
            ],
        )

        expected_df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": True,
                "Price": [3.3 * i if i < row_num2 else 2.2 * i for i in range(row_num)],
                "Label": [f"ABC{i}" for i in range(row_num)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.BATCH_ID: [
                    batch_id3 if i < row_num2 else batch_id2 for i in range(row_num)
                ],
            }
        )

        compare_data_frames(expected_df, reloaded_df.sort_values(by="Security"))

    @use_temp_folder
    def test_check_load_filters(self):
        table_name = "mytesttable"
        sqlite_manager = SqliteDataManager(os.path.join(self.temp_directory, "test.db"))

        calculator = PandasCalculator("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
            column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
        }

        sqlite_manager.create_table(table_name, schema, None)

        row_num = 10
        df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": [i % 2 == 0 for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                "Label": [f"ABC{i}" for i in range(row_num)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.INSERTED_DATETIME: unix_now(),
                "InsertedDatetime2": datetime.now().replace(microsecond=0),
                column_names.BATCH_ID: 10000,
            }
        )

        sqlite_manager.add_data_to_table(calculator, table_name, schema, {}, df)

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 3.3), ("Clean", "=", True)]],
            columns=["Security", "Clean", "Price", "Label"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC2"],
                "Clean": [True, True],
                "Price": [0.0, 2.2],
                "Label": ["ABC0", "ABC2"],
            }
        )

        compare_data_frames(expected_df, reloaded_df.sort_values(by="Security"))

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 3.3)], [("Clean", "=", False)]],
            columns=["Security", "Clean", "Price", "Label"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1", "ABC2", "ABC3", "ABC5", "ABC7", "ABC9"],
                "Clean": [True, False, True, False, False, False, False],
                "Price": [0.0, 1.1, 2.2, 3.3, 5.5, 7.7, 9.9],
                "Label": ["ABC0", "ABC1", "ABC2", "ABC3", "ABC5", "ABC7", "ABC9"],
            }
        )

        reloaded_df["Price"] = reloaded_df["Price"].round(4)
        compare_data_frames(expected_df, reloaded_df.sort_values(by="Security"))

    @use_temp_folder
    def test_check_load_filters_with_index(self):
        table_name = "mytesttable"
        sqlite_manager = SqliteDataManager(os.path.join(self.temp_directory, "test.db"))

        calculator = PandasCalculator("test")

        schema = {
            "Security": (Symbol, ColumnCategory.KEY),
            "Clean": (bool, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
            "Label": (str, ColumnCategory.VALUE),
            column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
            column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
            "InsertedDatetime2": (datetime, ColumnCategory.GENERIC),
            column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
        }

        sqlite_manager.create_table(table_name, schema, ["Security", "Label"])

        row_num = 10
        df = pd.DataFrame(
            {
                "Security": [f"ABC{i}" for i in range(row_num)],
                "Clean": [i % 2 == 0 for i in range(row_num)],
                "Price": [1.1 * i for i in range(row_num)],
                "Label": [f"ABC{i}" for i in range(row_num)],
                column_names.DATA_DATE: datetime.today().date(),
                column_names.INSERTED_DATETIME: unix_now(),
                "InsertedDatetime2": datetime.now().replace(microsecond=0),
                column_names.BATCH_ID: 10000,
            }
        )

        sqlite_manager.add_data_to_table(calculator, table_name, schema, {}, df)

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 3.3), ("Clean", "=", True)]],
            columns=["Security", "Clean", "Price", "Label"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC2"],
                "Clean": [True, True],
                "Price": [0.0, 2.2],
                "Label": ["ABC0", "ABC2"],
            }
        )

        compare_data_frames(expected_df, reloaded_df.sort_values(by="Security"))

        reloaded_df = sqlite_manager.load_data_from_database(
            calculator,
            table_name,
            schema,
            filters=[[("Price", "<", 3.3)], [("Clean", "=", False)]],
            columns=["Security", "Clean", "Price", "Label"],
        )

        expected_df = pd.DataFrame(
            {
                "Security": ["ABC0", "ABC1", "ABC2", "ABC3", "ABC5", "ABC7", "ABC9"],
                "Clean": [True, False, True, False, False, False, False],
                "Price": [0.0, 1.1, 2.2, 3.3, 5.5, 7.7, 9.9],
                "Label": ["ABC0", "ABC1", "ABC2", "ABC3", "ABC5", "ABC7", "ABC9"],
            }
        )

        reloaded_df["Price"] = reloaded_df["Price"].round(4)
        compare_data_frames(expected_df, reloaded_df.sort_values(by="Security"))
