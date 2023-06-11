import os
from abc import ABC
from datetime import date, datetime

import pandas as pd

from nodalize.calculators.dask_calculator import DaskCalculator
from nodalize.calculators.pandas_calculator import PandasCalculator
from nodalize.calculators.polars_calculator import PolarsCalculator
from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from nodalize.calculators.spark_calculator import SparkCalculator
from nodalize.tools.dates import unix_now
from tests.common import compare_data_frames


class BaseTestCalculator(ABC):
    @property
    def calculator_type(self):
        raise NotImplementedError

    @property
    def calculator_types(self):
        return [
            PandasCalculator,
            DaskCalculator,
            SparkCalculator,
            PyarrowCalculator,
            PolarsCalculator,
        ]

    @property
    def temp_directory(self):
        raise NotImplementedError

    def run_test_interoperability(self, partitioning):
        saver = self.calculator_type("test")
        table = "mytable"
        now = unix_now()

        pandas_df = pd.DataFrame()
        pandas_df["Id"] = [1, 5, 8]
        pandas_df["DataDate"] = [
            datetime(2022, 1, 1).date(),
            datetime(2022, 1, 2).date(),
            datetime(2022, 1, 3).date(),
        ]
        pandas_df["Price"] = [100.1, 101.2, 102.3]
        pandas_df["Label"] = ["ABC", "DEF", "ABC"]
        pandas_df["InsertedDatetime"] = [now, now, now]
        schema = {
            "Id": int,
            "DataDate": date,
            "Price": float,
            "Label": str,
            "InsertedDatetime": int,
        }

        target_df = saver.from_pandas(pandas_df, dask_npartitions=2)
        pandas_df = pandas_df[["Id", "DataDate", "Label", "Price", "InsertedDatetime"]]

        file_path = os.path.join(self.temp_directory, table)

        if not os.path.exists(self.temp_directory):
            os.makedirs(self.temp_directory, exist_ok=True)

        print(f"Saving parquet file using {saver.__class__.__name__}")

        partitions = None
        if partitioning:
            partitions = ["Id"]

        saver.save_parquet(file_path, target_df, schema, partitions)

        for loader_type in [
            t for t in self.calculator_types if t != self.calculator_type
        ]:
            # Polars does not support partitioning yet
            if partitioning and (
                loader_type == PolarsCalculator
                or self.calculator_type == PolarsCalculator
            ):
                continue

            loader = loader_type("test")
            print(f"Loading file using {loader.__class__.__name__}")

            reloaded_df = loader.load_parquet([file_path], schema)

            pandas_df2 = loader.to_pandas(reloaded_df)
            pandas_df2 = pandas_df2.sort_values(["Id", "DataDate"])

            compare_data_frames(pandas_df, pandas_df2)
