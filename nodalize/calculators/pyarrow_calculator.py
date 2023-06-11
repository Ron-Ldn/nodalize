"""Pyarrow calculator."""
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from nodalize.calculators.calculator import Calculator
from nodalize.constants.custom_types import Symbol


class PyarrowCalculator(Calculator[pa.Table]):
    """Pyarrow calculator."""

    calculator_type = "pyarrow"

    def __init__(self, app_name: str) -> None:
        """
        Initialize calculator.

        Args:
            app_name: application name
        """
        Calculator.__init__(self, app_name)

    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> pa.Table:
        """
        Convert from pandas data frame to pyarrow table.

        Args:
            dataframe: pandas data frame

        Returns:
            pyarrow table
        """
        df = pa.Table.from_pandas(dataframe)
        return self.select_columns(df, dataframe.columns.tolist())

    def to_pandas(self, dataframe: pa.Table) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Args:
            dataframe: pyarrow table

        Returns:
            pandas data frame
        """
        return dataframe.to_pandas()

    def create_data_frame(
        self,
        values: Dict[str, List[Any]],
        types: Dict[str, type],
    ) -> pa.Table:
        """
        Create data frame from column values.

        Args:
            values: column values
            types: column types

        Returns:
            data frame
        """
        return pa.Table.from_arrays(list(values.values()), names=list(values.keys()))

    def column_exists(self, dataframe: pa.Table, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        return column_name in dataframe.column_names

    def get_column(self, dataframe: pa.Table, column_name: str) -> Any:
        """
        Get column object, to make basic calculations and reinsert using add_column.

        Args:
            dataframe: data frame
            column_name: column name to get

        Returns:
            column object
        """
        return dataframe[column_name].to_numpy()

    @staticmethod
    def convert_type_for_pyarrow(t: type) -> pa.DataType:
        """
        Convert from native python type to pyarrow data type.

        Args:
            t: python type

        Returns:
            pyarrow data type
        """
        if t == int:
            return pa.int64()
        elif t == float:
            return pa.float64()
        elif t == str or t == Symbol:
            return pa.string()
        elif t == bool:
            return pa.bool_()
        elif t == date:
            return pa.date64()
        elif t == datetime:
            return pa.timestamp("ns")
        else:
            raise NotImplementedError(f"Cannot convert type {t} to PyArrow DataType")

    @staticmethod
    def convert_type_from_pyarrow(t: pa.DataType) -> type:
        """
        Convert from pyarrow data type to native python type.

        Args:
            t: pyarrow data type

        Returns:
            python type
        """
        if t in [pa.int16(), pa.int32(), pa.int8(), pa.int64()]:
            return int
        elif t in [pa.float16(), pa.float32(), pa.float64()]:
            return float
        elif t == pa.string():
            return str
        elif t == pa.bool_():
            return bool
        elif t in [pa.date64(), pa.date32()]:
            return date
        elif t in [pa.timestamp("ns"), pa.timestamp("us")]:
            return datetime
        else:
            raise NotImplementedError(
                f"Cannot convert PyArrow DataType {t} to Python type"
            )

    def add_column(
        self,
        dataframe: pa.Table,
        column_name: str,
        value: Any,
        literal: bool = False,
        override: bool = True,
    ) -> pa.Table:
        """
        Add column to data frame, with same default value on each rows.

        Args:
            dataframe: pyarrow table
            column_name: new column name
            value: value to assign to the new column
            literal: if True then will set the same value for the entire column, otherwise will consider it as a column
            override: if False, then will not try to replace an existing column

        Returns:
            pyarrow table
        """
        if self.column_exists(dataframe, column_name):
            if override:
                dataframe = self.drop_columns(dataframe, [column_name])
            else:
                return dataframe

        if literal:
            if dataframe.num_rows > 0:
                dataframe = dataframe.append_column(
                    column_name, pa.array([value] * len(dataframe))
                )
            else:
                dataframe = dataframe.append_column(
                    column_name,
                    pa.array(
                        [value] * len(dataframe),
                        self.convert_type_for_pyarrow(value.__class__),
                    ),
                )
        else:
            if isinstance(value, pa.Array) or isinstance(value, pa.ChunkedArray):
                array = value
            else:
                array = pa.array(value)
            dataframe = dataframe.append_column(column_name, array)

        return dataframe

    def filter_in_max_values(
        self, dataframe: pa.Table, value_columns: List[str], key_columns: List[str]
    ) -> pa.Table:
        """
        Filter on max values from the data frame.

        Value columns are typically date columns (InsertedDateTime).

        Args:
            dataframe: pyarrow table
            value_columns: list of columns for which we look for the maximum values
            key_columns: columns to group by on, before searching for the maximum values

        Returns:
            pyarrow table
        """
        key_columns = key_columns.copy()
        aggregation = dataframe
        for vc in value_columns:
            max_series = aggregation.group_by(key_columns).aggregate([(vc, "max")])
            max_series = max_series.rename_columns(
                [vc if c == f"{vc}_max" else c for c in max_series.column_names]
            )
            aggregation = max_series.join(
                aggregation, key_columns + [vc], join_type="left outer"
            )
            key_columns.append(vc)
        return aggregation

    def drop_columns(self, dataframe: pa.Table, columns: Iterable[str]) -> pa.Table:
        """
        Drop columns from data frame.

        Args:
            dataframe: pyarrow table
            columns: list of columns to remove

        Returns:
            pyarrow table
        """
        return dataframe.drop(columns)

    def select_columns(self, dataframe: pa.Table, columns: Iterable[str]) -> pa.Table:
        """
        Select columns in data frame.

        Args:
            dataframe: pyarrow table
            columns: list of columns to keep

        Returns:
            pyarrow table
        """
        return dataframe.select(columns)

    def drop_duplicates(self, dataframe: pa.Table) -> pa.Table:
        """
        Drop duplicates.

        Args:
            dataframe: pyarrow table

        Returns:
            pyarrow table
        """
        return dataframe.group_by(dataframe.column_names).aggregate([])

    def concat(self, dataframes: List[pa.Table]) -> pa.Table:
        """
        Concatenate data frames.

        Args:
            dataframes: list of data frames to concatenate

        Returns:
            data frame
        """
        if len(dataframes) > 1:
            columns = dataframes[0].column_names
            dataframes = [df.select(columns) for df in dataframes]
        return pa.concat_tables(dataframes)

    def apply_filter(
        self, df: pa.Table, filter: Tuple[str, str, Any], schema: Dict[str, type]
    ) -> pa.Table:
        """
        Apply filter to data frame and return new instance.

        Args:
            df: data frame
            filter: filter
            schema: schema

        Returns:
            new data frame
        """
        if len(df) == 0:
            return df

        left_op = df[filter[0]]
        operator = filter[1]
        right_op = filter[2]

        if operator == "=":
            return df.filter(pc.equal(left_op, right_op))
        elif operator == "!=":
            return df.filter(pc.not_equal(left_op, right_op))
        elif operator == "<=":
            return df.filter(pc.less_equal(left_op, right_op))
        elif operator == "<":
            return df.filter(pc.less(left_op, right_op))
        elif operator == ">=":
            return df.filter(pc.greater_equal(left_op, right_op))
        elif operator == ">":
            return df.filter(pc.greater(left_op, right_op))
        elif operator == "in":
            len_df = len(df)
            bool_array = pa.array([False] * len_df)

            for v in right_op:
                val_array = pa.array([v] * len_df)
                bool_array = pc.or_(bool_array, pc.equal(left_op, val_array))

            df = df.filter(bool_array)
            return df
        elif operator == "not in":
            len_df = len(df)
            bool_array = pa.array([True] * len_df)

            for v in right_op:
                val_array = pa.array([v] * len_df)
                bool_array = pc.and_(bool_array, pc.not_equal(left_op, val_array))

            df = df.filter(bool_array)
            return df
        else:
            raise NotImplementedError(f"Unknown filter operator: {operator}")

    def _apply_and_filters(
        self, df: pa.Table, filters: List[Tuple[str, str, Any]], schema: Dict[str, type]
    ) -> pa.Table:
        """
        Apply ABD filters to data frame and return new instance.

        Args:
            df: data frame
            filters: filters
            schema: schema

        Returns:
            new data frame
        """
        for filter in filters:
            df = self.apply_filter(df, filter, schema)
        return df

    def apply_filters(
        self,
        df: pa.Table,
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, type],
    ) -> pa.Table:
        """
        Apply filters to data frame and return new instance.

        Args:
            df: data frame
            filters: filters
            schema: schema

        Returns:
            new data frame
        """
        if len(filters) == 0:
            raise AssertionError("No filter provided")
        elif len(filters) == 1:
            return self._apply_and_filters(df, filters[0], schema)
        else:
            row_num_column = self.generate_temporary_column_name(df)
            df = df.append_column(row_num_column, pa.array(np.arange(len(df))))
            dfs = [self._apply_and_filters(df, filter, schema) for filter in filters]
            df = pa.concat_tables(dfs)
            df = self.drop_duplicates(df)
            df = df.drop([row_num_column])
            return df

    @staticmethod
    def _empty_table(dataframe: pa.Table) -> pa.Table:
        """
        Make data frame empty.

        Args:
            dataframe: data frame

        Return:
            empty data frame with same column types
        """
        first_col = dataframe.column_names[0]
        return dataframe.filter(
            pc.not_equal(dataframe[first_col], dataframe[first_col])
        )

    @staticmethod
    def _add_columns(dataframe: pa.Table, columns: Dict[str, pa.DataType]) -> pa.Table:
        """
        Add empty columns to data frame.

        Args:
            dataframe: data frame

        Return:
            same data frame with new columns, remaining empty
        """
        for n, t in columns.items():
            if n not in dataframe.column_names:
                new_column = pa.array([None] * len(dataframe), type=t)
                dataframe = dataframe.append_column(n, new_column)

        return dataframe

    @staticmethod
    def _get_column_types(dataframe: pa.Table) -> Dict[str, pa.DataType]:
        """
        Get column types from data frame.

        Args:
            dataframe: data frame

        Return:
            dictionary name/type
        """
        return {
            name: dataframe[name].type
            for name in dataframe.column_names
            if not name.startswith("__") and not name.endswith("__")
        }

    def _join_non_empty_tables(
        self, left: pa.Table, right: pa.Table, on: List[str]
    ) -> pa.Table:
        """
        Join 2 non-empty data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        # Handle cases where 2 columns to merge have the same type but one is flagged as object in Pandas
        for cname in on:
            if left[cname].type != right[cname].type:
                left = self.add_column(
                    left, cname, pc.cast(left[cname], right[cname].type)
                )

        joined_table = left.join(
            right,
            join_type="left outer",
            keys=on,
            left_suffix="",
            right_suffix="_right",
            coalesce_keys=False,
        )

        return joined_table

    def left_join_data_frames(
        self, left: pa.Table, right: pa.Table, on: List[str]
    ) -> pa.Table:
        """
        Join 2 data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        # Note: pyarrow does not support joining with empty tables.
        # Let's work around it.
        if left.num_rows == 0 or right.num_rows == 0:
            return self._add_columns(left, self._get_column_types(right))
        else:
            return self._join_non_empty_tables(left, right, on)

    def rename_columns(self, dataframe: pa.Table, names: Dict[str, str]) -> pa.Table:
        """
        Rename columns in data frame.

        Args:
            dataframe: pyarrow table
            names: dictionary of column names - before: after

        Returns:
            pyarrow table
        """
        new_names = [names.get(name) or name for name in dataframe.column_names]
        return dataframe.rename_columns(new_names)

    def extract_unique_column_values(
        self, column_name: str, dataframe: pa.Table
    ) -> Set[Any]:
        """
        Get existing values from a given column in the data frame.

        Args:
            column_name: name of the column
            dataframe: dataframe

        Returns:
            set of values
        """
        # todo the code below works but keep the data as pyarrow format - not sure how to convert back.
        # For instance, dates are Date32Scalar.
        # We might have to check the data type and convert manually...
        # return set(pc.unique(dataframe[column_name]))

        pandas_series = self.to_pandas(dataframe[column_name])
        return set(pd.unique(pandas_series))  # type: ignore

    @staticmethod
    def convert_schema_for_pyarrow(schema: Dict[str, type]) -> pa.schema:
        """
        Convert schema for pyarrow: replace native types with numpy types.

        Args:
            schema: dictionary of (name: str, type: type)

        Returns:
            dictionary of (name: str, type: np.dtype)
        """
        fields = [
            pa.field(k, PyarrowCalculator.convert_type_for_pyarrow(v))
            for k, v in schema.items()
        ]
        return pa.schema(fields)

    @staticmethod
    def convert_schema_from_pyarrow(schema: pa.schema) -> Dict[str, type]:
        """
        Convert schema from pyarrow.

        Args:
            schema: dictionary of (name: str, type: np.dtype)

        Returns:
            dictionary of (name: str, type: type)
        """
        return {
            k: PyarrowCalculator.convert_type_from_pyarrow(v)
            for k, v in zip(schema.names, schema.types)
        }

    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[pa.Table]:
        """
        Load data frame from parquet files.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            pyarrow table
        """

        def read_file(file: str) -> pa.Table:
            table = pq.ParquetDataset(
                file, use_legacy_dataset=False, filters=filters
            ).read()

            if columns is not None:
                table = table.select(columns)

            return table

        dfs = [read_file(file) for file in file_paths]

        if len(dfs) > 0:
            columns = [
                c
                for c in dfs[0].column_names
                if not c.startswith("__") and not c.endswith("__")
            ]
            dfs = [df.select(columns) for df in dfs]

        df = pa.concat_tables(dfs)

        return df

    def save_parquet(
        self,
        file_path: str,
        dataframe: pa.Table,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.

        Args:
            file_path: path to the file
            dataframe: pyarrow table
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        pq.write_to_dataset(dataframe, root_path=file_path, partition_cols=partitions)

    def row_count(self, dataframe: pa.Table) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        return len(dataframe)
