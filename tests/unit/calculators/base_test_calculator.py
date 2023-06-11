from abc import ABC
from datetime import date, datetime

import numpy as np
import pandas as pd

from nodalize.tools.dates import unix_now
from tests.common import compare_data_frames


class BaseTestCalculator(ABC):
    def test_pandas_conversion(self):
        now = unix_now()

        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 5, 8]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1).date(),
            datetime(2022, 1, 2).date(),
            datetime(2022, 1, 3).date(),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3]
        pandas_df["InsertedDatetime"] = [now, now, now]

        calculator = self.calculator_type("test")
        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        pandas_df2 = calculator.to_pandas(target_df)

        compare_data_frames(pandas_df, pandas_df2)

    def test_create_data_frame(self):
        calculator = self.calculator_type("test")
        now = datetime.now()

        values = {
            "Id": [1, 5, 8],
            "DataDate": [
                datetime(2022, 1, 1).date(),
                datetime(2022, 1, 2).date(),
                datetime(2022, 1, 3).date(),
            ],
            "Price": [100.1, 101.2, 102.3],
            "Label": ["A", "B", "C"],
            "InsertedDatetime": [now, now, now],
        }

        types = {
            "Id": int,
            "DataDate": date,
            "Price": float,
            "Label": str,
            "InsertedDatetime": datetime,
        }

        df = calculator.create_data_frame(values, types)

        expected_df = pd.DataFrame(
            {
                "Id": [1, 5, 8],
                "DataDate": [
                    datetime(2022, 1, 1).date(),
                    datetime(2022, 1, 2).date(),
                    datetime(2022, 1, 3).date(),
                ],
                "Price": [100.1, 101.2, 102.3],
                "Label": ["A", "B", "C"],
                "InsertedDatetime": [now, now, now],
            }
        )

        ret_df = calculator.to_pandas(df)
        ret_df["Price"] = ret_df["Price"].round(5)  # Spark may be funny sometimes
        compare_data_frames(expected_df, ret_df)

    def test_column_exists(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3]
        pandas_df["Value"] = [100.1, 101.2, 102.3]

        calculator = self.calculator_type("test")
        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.assertTrue(calculator.column_exists(target_df, "Id"))
        self.assertFalse(calculator.column_exists(target_df, "Date"))

    def test_add_column(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3]
        pandas_df["Value"] = [100.1, 101.2, 102.3]

        calculator = self.calculator_type("test")

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        target_df = calculator.add_column(target_df, "StaticCol", 1.2, literal=True)
        target_df = calculator.add_column(
            target_df,
            "DynamicCol",
            calculator.get_column(target_df, "Value") * 2,
            literal=False,
        )

        expected_pandas_df = pd.DataFrame()
        expected_pandas_df["Id"] = [1, 2, 3]
        expected_pandas_df["Value"] = [100.1, 101.2, 102.3]
        expected_pandas_df["StaticCol"] = [1.2, 1.2, 1.2]
        expected_pandas_df["DynamicCol"] = [200.2, 202.4, 204.6]

        compare_data_frames(expected_pandas_df, calculator.to_pandas(target_df))

        target_df = calculator.add_column(target_df, "StaticCol", 1.3, literal=True)
        target_df = calculator.add_column(
            target_df,
            "DynamicCol",
            calculator.get_column(target_df, "Id") * 3,
            literal=False,
        )

        expected_pandas_df = pd.DataFrame()
        expected_pandas_df["Id"] = [1, 2, 3]
        expected_pandas_df["Value"] = [100.1, 101.2, 102.3]
        expected_pandas_df["StaticCol"] = [1.3, 1.3, 1.3]
        expected_pandas_df["DynamicCol"] = [3, 6, 9]

        compare_data_frames(expected_pandas_df, calculator.to_pandas(target_df))

        target_df = calculator.add_column(
            target_df, "StaticCol", 1.4, override=False, literal=True
        )
        compare_data_frames(expected_pandas_df, calculator.to_pandas(target_df))

    def test_drop_columns(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3]
        pandas_df["Price1"] = [100.1, 101.2, 102.3]
        pandas_df["Price2"] = [200.1, 201.2, 202.3]
        pandas_df["Price3"] = [300.1, 301.2, 302.3]

        calculator = self.calculator_type("test")

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        target_df = calculator.drop_columns(target_df, ["Price1", "Price2"])

        expected_pandas_df = pd.DataFrame()
        expected_pandas_df["Id"] = [1, 2, 3]
        expected_pandas_df["Price3"] = [300.1, 301.2, 302.3]

        compare_data_frames(expected_pandas_df, calculator.to_pandas(target_df))

    def test_select_columns(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3]
        pandas_df["Price1"] = [100.1, 101.2, 102.3]
        pandas_df["Price2"] = [200.1, 201.2, 202.3]
        pandas_df["Price3"] = [300.1, 301.2, 302.3]

        calculator = self.calculator_type("test")

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        target_df = calculator.select_columns(target_df, ["Price1", "Price2"])

        expected_pandas_df = pd.DataFrame()
        expected_pandas_df["Price1"] = [100.1, 101.2, 102.3]
        expected_pandas_df["Price2"] = [200.1, 201.2, 202.3]

        compare_data_frames(expected_pandas_df, calculator.to_pandas(target_df))

    def test_drop_duplicates(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 1, 3, 3, 4, 3, 1]
        pandas_df["Value"] = [1.1, 1.2, 3.3, 3.3, 4.4, 3.3, 1.2]

        calculator = self.calculator_type("test")

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        target_df = calculator.drop_duplicates(target_df)

        expected_pandas_df = pd.DataFrame()
        expected_pandas_df["Id"] = [1, 1, 3, 4]
        expected_pandas_df["Value"] = [1.1, 1.2, 3.3, 4.4]

        compare_data_frames(
            expected_pandas_df,
            calculator.to_pandas(target_df).sort_values(["Id", "Value"]),
        )

    def test_concat(self):
        calculator = self.calculator_type("test")

        df1 = calculator.from_pandas(
            pd.DataFrame({"Id": [1, 2], "Price": [1.1, 2.2]}), dask_npartitions=2
        )

        df2 = calculator.from_pandas(
            pd.DataFrame({"Id": [3], "Price": [3.3]}), dask_npartitions=2
        )

        pandas_df3 = pd.DataFrame({"Id": [33], "Price": [33.3]})
        pandas_df3 = pandas_df3.loc[pandas_df3["Id"] == 4]
        df3 = calculator.from_pandas(pandas_df3, dask_npartitions=2)

        df4 = calculator.from_pandas(
            pd.DataFrame({"Id": [4], "Price": [4.4]}), dask_npartitions=2
        )

        target_df = calculator.concat([df1, df2, df3, df4])

        expected_df = pd.DataFrame({"Id": [1, 2, 3, 4], "Price": [1.1, 2.2, 3.3, 4.4]})

        compare_data_frames(
            expected_df,
            calculator.to_pandas(target_df).sort_values("Id"),
        )

    def test_apply_filter(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3, 4]
        pandas_df["Value"] = ["A", "B", "C", "D"]

        calculator = self.calculator_type("test")
        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        def test_filter(filter, expected_values):
            expected_df = pd.DataFrame()
            ids = list(expected_values.keys())
            values = [expected_values[i] for i in ids]
            expected_df["Id"] = ids
            expected_df["Value"] = values

            final_df = calculator.apply_filter(
                target_df, filter, {"Id": int, "Value": str}
            )
            compare_data_frames(
                expected_df.sort_values("Id"),
                calculator.to_pandas(final_df).sort_values("Id"),
            )

        test_filter(
            ("Id", "=", 2),
            {2: "B"},
        )
        test_filter(
            ("Value", "=", "B"),
            {2: "B"},
        )
        test_filter(
            ("Id", "!=", 2),
            {1: "A", 3: "C", 4: "D"},
        )
        test_filter(
            ("Value", "!=", "B"),
            {1: "A", 3: "C", 4: "D"},
        )
        test_filter(
            ("Id", "<=", 3),
            {1: "A", 2: "B", 3: "C"},
        )
        test_filter(
            ("Id", "<", 3),
            {1: "A", 2: "B"},
        )
        test_filter(
            ("Id", ">=", 2),
            {2: "B", 3: "C", 4: "D"},
        )
        test_filter(
            ("Id", ">", 2),
            {3: "C", 4: "D"},
        )
        test_filter(
            ("Id", "not in", [2, 4]),
            {1: "A", 3: "C"},
        )
        test_filter(
            ("Value", "not in", ["B", "D"]),
            {1: "A", 3: "C"},
        )
        test_filter(
            ("Id", "in", [2, 4]),
            {2: "B", 4: "D"},
        )
        test_filter(
            ("Value", "in", ["B", "D"]),
            {2: "B", 4: "D"},
        )

    def test_filter_in_max_values(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2, 1, 2, 1, 1, 2, 1, 2, 1]
        pandas_df["Id2"] = [3, 3, 3, 3, 4, 3, 3, 3, 3, 4]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
            datetime(2022, 1, 3),
            datetime(2022, 1, 4),
            datetime(2022, 1, 5),
            datetime(2022, 1, 1),
            datetime(2022, 1, 2),
            datetime(2022, 1, 3),
            datetime(2022, 1, 4),
            datetime(2022, 1, 5),
        ]
        pandas_df["AsOfDatetime"] = [
            datetime(2022, 2, 1, 1, 0, 1),
            datetime(2022, 2, 1, 1, 0, 2),
            datetime(2022, 2, 1, 1, 0, 3),
            datetime(2022, 2, 1, 1, 0, 4),
            datetime(2022, 2, 1, 1, 0, 5),
            datetime(2022, 2, 1, 2, 0, 1),
            datetime(2022, 2, 1, 2, 0, 2),
            datetime(2022, 2, 1, 2, 0, 3),
            datetime(2022, 2, 1, 2, 0, 4),
            datetime(2022, 2, 1, 2, 0, 5),
        ]
        pandas_df["Price"] = [
            100.1,
            101.2,
            102.3,
            103.4,
            104.5,
            110.1,
            111.2,
            112.3,
            113.4,
            114.5,
        ]

        calculator = self.calculator_type("test")

        target_df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        target_df = calculator.filter_in_max_values(
            target_df, ["DataDate", "AsOfDatetime"], ["Id1", "Id2"]
        )

        expected_df = pd.DataFrame()
        expected_df["Id1"] = [1, 2, 1]
        expected_df["Id2"] = [3, 3, 4]
        expected_df["DataDate"] = [
            datetime(2022, 1, 3),
            datetime(2022, 1, 4),
            datetime(2022, 1, 5),
        ]
        expected_df["AsOfDatetime"] = [
            datetime(2022, 2, 1, 2, 0, 3),
            datetime(2022, 2, 1, 2, 0, 4),
            datetime(2022, 2, 1, 2, 0, 5),
        ]
        expected_df["Price"] = [112.3, 113.4, 114.5]

        columns_for_sorting = list(expected_df.columns.values)
        compare_data_frames(
            expected_df.sort_values(columns_for_sorting),
            calculator.to_pandas(target_df).sort_values(columns_for_sorting),
        )

    def test_left_join(self):
        pandas_df_left = pd.DataFrame()
        pandas_df_left["Id1"] = [1, 1, 2, 2]
        pandas_df_left["Id2"] = ["A", "B", "C", "D"]
        pandas_df_left["Price"] = [1.23, 2.34, 3.45, 4.56]

        pandas_df_right = pd.DataFrame()
        pandas_df_right["Id1"] = [1, 1, 2, 3]
        pandas_df_right["Id2"] = ["A", "C", "C", "E"]
        pandas_df_right["Label"] = ["label1", "label2", "label3", "label4"]

        calculator = self.calculator_type("test")

        left_df = calculator.from_pandas(pandas_df_left, dask_npartitions=2)
        right_df = calculator.from_pandas(pandas_df_right, dask_npartitions=2)

        joined_df = calculator.left_join_data_frames(left_df, right_df, ["Id1", "Id2"])
        joined_df_pandas = calculator.to_pandas(joined_df)

        expected_df = pd.DataFrame()
        expected_df["Id1"] = [1, 1, 2, 2]
        expected_df["Id2"] = ["A", "B", "C", "D"]
        expected_df["Price"] = [1.23, 2.34, 3.45, 4.56]
        expected_df["Label"] = ["label1", None, "label3", None]

        compare_data_frames(
            expected_df.sort_values(["Id1", "Id2"]),
            joined_df_pandas.sort_values(["Id1", "Id2"]),
        )

    def test_left_join_with_empty_dataframe(self):
        pandas_df_left = pd.DataFrame()
        pandas_df_left["Id1"] = [1]
        pandas_df_left["Id2"] = ["A"]
        pandas_df_left["Price"] = [1.23]
        pandas_df_left = pandas_df_left.loc[pandas_df_left["Id1"] == 2]

        pandas_df_right = pd.DataFrame()
        pandas_df_right["Id1"] = [1]
        pandas_df_right["Id2"] = ["A"]
        pandas_df_right["Label"] = ["Label1"]

        calculator = self.calculator_type("test")

        left_df = calculator.from_pandas(pandas_df_left, dask_npartitions=2)
        right_df = calculator.from_pandas(pandas_df_right, dask_npartitions=2)

        joined_df = calculator.left_join_data_frames(left_df, right_df, ["Id1", "Id2"])
        joined_df_pandas = calculator.to_pandas(joined_df)

        expected_df = pd.DataFrame()
        expected_df["Id1"] = []
        expected_df["Id2"] = []
        expected_df["Price"] = []
        expected_df["Label"] = []

        compare_data_frames(
            expected_df.sort_values(["Id1", "Id2"]),
            joined_df_pandas.sort_values(["Id1", "Id2"]),
        )

        joined_df = calculator.left_join_data_frames(right_df, left_df, ["Id1", "Id2"])
        joined_df_pandas = calculator.to_pandas(joined_df)

        expected_df = pd.DataFrame()
        expected_df["Id1"] = [1]
        expected_df["Id2"] = ["A"]
        expected_df["Price"] = [None]
        expected_df["Label"] = ["Label1"]

        compare_data_frames(
            expected_df.sort_values(["Id1", "Id2"]),
            joined_df_pandas.sort_values(["Id1", "Id2"]),
        )

    def test_rename_columns(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id1"] = [1, 2]
        pandas_df["Id2"] = [3, 4]
        pandas_df["Id3"] = [5, 6]
        pandas_df["Id4"] = [7, 8]

        calculator = self.calculator_type("test")
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        df = calculator.rename_columns(df, {"Id1": "Name1", "Id3": "Name3"})

        expected_df = pd.DataFrame()
        expected_df["Name1"] = [1, 2]
        expected_df["Id2"] = [3, 4]
        expected_df["Name3"] = [5, 6]
        expected_df["Id4"] = [7, 8]

        target_df = calculator.to_pandas(df)
        compare_data_frames(expected_df, target_df)

    def test_extract_unique_column_values(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3, 1, 2]
        pandas_df["Date"] = [
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 3),
            date(2022, 1, 1),
            date(2022, 1, 2),
        ]

        calculator = self.calculator_type("test")
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        values = calculator.extract_unique_column_values("Date", df)
        self.assertEqual({date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)}, values)

    def test_apply_filters(self):
        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 2, 3, 1, 2]
        pandas_df["Date"] = [
            date(2022, 1, 1),
            date(2022, 1, 1),
            date(2022, 1, 1),
            date(2022, 1, 2),
            date(2022, 1, 2),
        ]
        pandas_df["Value"] = [
            1.1,
            2.2,
            3.3,
            4.4,
            5.5,
        ]

        calculator = self.calculator_type("test")
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)
        filtered_df = calculator.apply_filters(
            df,
            [[("Id", "in", [1, 2]), ("Date", "=", date(2022, 1, 1))]],
            {"Id": int, "Value": str, "Date": date},
        )
        target_df = calculator.to_pandas(filtered_df)

        expected_df = pd.DataFrame()
        expected_df["Id"] = [1, 2]
        expected_df["Date"] = [
            date(2022, 1, 1),
            date(2022, 1, 1),
        ]
        expected_df["Value"] = [
            1.1,
            2.2,
        ]

        compare_data_frames(expected_df, target_df.sort_values(by=["Id"]))

        filtered_df = calculator.apply_filters(
            df,
            [[("Id", "=", 1)], [("Date", "=", date(2022, 1, 1))]],
            {"Id": int, "Value": str, "Date": date},
        )
        target_df = calculator.to_pandas(filtered_df)

        expected_df = pd.DataFrame()
        expected_df["Id"] = [1, 2, 3, 1]
        expected_df["Date"] = [
            date(2022, 1, 1),
            date(2022, 1, 1),
            date(2022, 1, 1),
            date(2022, 1, 2),
        ]
        expected_df["Value"] = [
            1.1,
            2.2,
            3.3,
            4.4,
        ]

        compare_data_frames(expected_df, target_df.sort_values(by=["Date", "Id"]))

    def test_row_count(self):
        calculator = self.calculator_type("test")

        pandas_df = pd.DataFrame({"Id": [1, 2, 3], "Value": [1.2, 2.3, 3.4]})
        df = calculator.from_pandas(pandas_df, dask_npartitions=2)

        self.assertEqual(3, calculator.row_count(df))

    def test_compute_correlation(self):
        calculator = self.calculator_type("test")
