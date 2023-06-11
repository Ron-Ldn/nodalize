"""Dask calculator."""
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast

import dask.dataframe as dd
import numpy as np
import pandas as pd

from nodalize.calculators.calculator import Calculator
from nodalize.calculators.pyarrow_calculator import PyarrowCalculator


class DaskCalculator(Calculator[dd.DataFrame]):
    """Dask calculator."""

    calculator_type = "dask"

    def __init__(self, app_name: str) -> None:
        """
        Initialize calculator.

        Args:
            app_name: application name
        """
        Calculator.__init__(self, app_name)

    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> dd.DataFrame:
        """
        Convert from pandas data frame to dask data frame.

        Args:
            dataframe: pandas data frame

        Returns:
            dask data frame
        """
        npartitions = kwargs.get("dask_npartitions") or 1

        df = cast(dd.DataFrame, dd.from_pandas(dataframe, npartitions=npartitions))
        return df

    def to_pandas(self, dataframe: dd.DataFrame) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Args:
            dataframe: dask data frame

        Returns:
            pandas data frame
        """
        return dataframe.compute()

    def create_data_frame(
        self,
        values: Dict[str, List[Any]],
        types: Dict[str, type],
    ) -> dd.DataFrame:
        """
        Create data frame from column values.

        Args:
            values: column values
            types: column types

        Returns:
            data frame
        """
        if len(values) == 0:
            raise AssertionError("Cannot create empty data frame with Dask")

        df = None
        for header, value_list in values.items():
            if df is None:
                df = dd.from_array(np.array(value_list), columns=header).to_frame()
            else:
                df[header] = dd.from_array(np.array(value_list), columns=header)

        if df is None:
            raise AssertionError
        else:
            return df

    def column_exists(self, dataframe: dd.DataFrame, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        return column_name in dataframe.columns

    def get_column(self, dataframe: dd.DataFrame, column_name: str) -> Any:
        """
        Get column object, to make basic calculations and reinsert using add_column.

        Args:
            dataframe: data frame
            column_name: column name to get

        Returns:
            column object
        """
        return dataframe[column_name]

    def add_column(
        self,
        dataframe: dd.DataFrame,
        column_name: str,
        value: Any,
        literal: bool = False,
        override: bool = True,
    ) -> dd.DataFrame:
        """
        Add column to data frame, with same default value on each rows.

        Args:
            dataframe: dask data frame
            column_name: new column name
            value: value to assign to the new column
            literal: if True then will set the same value for the entire column, otherwise will consider it as a column
            override: if False, then will not try to replace an existing column

        Returns:
            dask data frame
        """
        if override or not self.column_exists(dataframe, column_name):
            dataframe[column_name] = value

        return dataframe

    def filter_in_max_values(
        self, dataframe: dd.DataFrame, value_columns: List[str], key_columns: List[str]
    ) -> dd.DataFrame:
        """
        Filter on max values from the data frame.

        Value columns are typically date columns (InsertedDateTime).

        Args:
            dataframe: dask data frame
            value_columns: list of columns for which we look for the maximum values
            key_columns: columns to group by on, before searching for the maximum values

        Returns:
            dask data frame
        """
        key_columns = key_columns.copy()
        temp_col_name = self.generate_temporary_column_name(dataframe)

        aggregation = dataframe
        for vc in value_columns:
            max_series = (
                aggregation.groupby(key_columns)
                .agg({vc: "max"})
                .reset_index()
                .rename(columns={vc: temp_col_name})
            )
            aggregation = aggregation.merge(max_series, how="left", on=key_columns)
            aggregation = aggregation.loc[
                aggregation[vc] == aggregation[temp_col_name]
            ].drop(temp_col_name, 1)
            key_columns.append(vc)
        return aggregation

    def drop_columns(
        self, dataframe: dd.DataFrame, columns: Iterable[str]
    ) -> dd.DataFrame:
        """
        Drop columns from data frame.

        Args:
            dataframe: dask data frame
            columns: list of columns to remove

        Returns:
            dask data frame
        """
        return dataframe.drop(list(columns), 1)

    def select_columns(
        self, dataframe: dd.DataFrame, columns: Iterable[str]
    ) -> dd.DataFrame:
        """
        Select columns in data frame.

        Args:
            dataframe: dask data frame
            columns: list of columns to keep

        Returns:
            dask data frame
        """
        return dataframe[list(columns)]

    def drop_duplicates(self, dataframe: dd.DataFrame) -> dd.DataFrame:
        """
        Drop duplicates.

        Args:
            dataframe: dask data frame

        Returns:
            pandas dask frame
        """
        return dataframe.drop_duplicates()

    def concat(self, dataframes: List[dd.DataFrame]) -> dd.DataFrame:
        """
        Concatenate data frames.

        Args:
            dataframes: list of data frames to concatenate

        Returns:
            data frame
        """
        if len(dataframes) > 1:
            columns = dataframes[0].columns
            dataframes = [df[columns] for df in dataframes]
        return dd.concat(dataframes)

    def apply_filter(
        self, df: dd.DataFrame, filter: Tuple[str, str, Any], schema: Dict[str, type]
    ) -> dd.DataFrame:
        """
        Apply filter to data frame and return new instance.

        Args:
            df: data frame
            filter: filter
            schema: schema

        Returns:
            new data frame
        """
        left_op = df[filter[0]]
        operator = filter[1]
        right_op = filter[2]

        if schema.get(filter[0]) == date:
            left_op = dd.to_datetime(left_op)

            def convert_date(dt):
                if isinstance(dt, date):
                    return datetime.combine(dt, datetime.min.time())
                else:
                    return dt

            if operator in ["in", "not in"]:
                right_op = [convert_date(v) for v in right_op]
            else:
                right_op = convert_date(right_op)

        if operator == "=":
            return df.loc[left_op == right_op]
        elif operator == "!=":
            return df.loc[left_op != right_op]
        elif operator == "<=":
            return df.loc[left_op <= right_op]
        elif operator == "<":
            return df.loc[left_op < right_op]
        elif operator == ">=":
            return df.loc[left_op >= right_op]
        elif operator == ">":
            return df.loc[left_op > right_op]
        elif operator == "in":
            return df.loc[left_op.isin(right_op)]
        elif operator == "not in":
            return df.loc[~left_op.isin(right_op)]
        else:
            raise NotImplementedError(f"Unknown filter operator: {operator}")

    def _apply_and_filters(
        self,
        df: dd.DataFrame,
        filters: List[Tuple[str, str, Any]],
        schema: Dict[str, type],
    ) -> dd.DataFrame:
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
        df: dd.DataFrame,
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, type],
    ) -> dd.DataFrame:
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
            df[row_num_column] = df.assign(partition_count=1).partition_count.cumsum()
            dfs = [self._apply_and_filters(df, filter, schema) for filter in filters]
            df = dd.multi.concat(dfs)
            df = df.drop_duplicates()
            df = df.drop(columns=row_num_column)
            return df

    def left_join_data_frames(
        self,
        left: dd.DataFrame,
        right: dd.DataFrame,
        on: List[str],
    ) -> dd.DataFrame:
        """
        Join 2 data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        # Handle cases where 2 columns to merge have the same type but one is flagged as object in Pandas
        for cname in on:
            if left[cname].dtype != right[cname].dtype:
                left[cname] = left[cname].astype(right[cname].dtype)

        return dd.merge(
            left,
            right,
            how="left",
            on=on,
            suffixes=("", "_right"),
        )

    def rename_columns(
        self, dataframe: dd.DataFrame, names: Dict[str, str]
    ) -> dd.DataFrame:
        """
        Rename columns in data frame.

        Args:
            dataframe: dask data frame
            names: dictionary of column names - before: after

        Returns:
            dask data frame
        """
        return dataframe.rename(columns=names)

    def extract_unique_column_values(
        self, column_name: str, dataframe: dd.DataFrame
    ) -> Set[Any]:
        """
        Get existing values from a given column in the data frame.

        Args:
            column_name: name of the column
            dataframe: dataframe

        Returns:
            set of values
        """
        return set(dataframe[column_name].unique())

    @staticmethod
    def convert_schema_for_dask(schema: Dict[str, type]) -> Dict[str, np.dtype]:
        """
         Convert schema for dask: replace native types with numpy types.

        Args:
            schema: dictionary of (name: str, type: type)

        Returns:
            dictionary of (name: str, type: np.dtype)
        """
        return {k: np.dtype(v) for k, v in schema.items()}

    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[dd.DataFrame]:
        """
        Load data frame from parquet files.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            dask data frame
        """
        # Note: tested with dask-2022.4.0
        # There is a bug using "not in" followed by a list.
        # For these use-cases, we will filter post-loading.
        apply_filters_on_load = (
            filters is None
            or len(filters) == 0
            or all(
                [
                    all(f[1].lower() != "not in" for f in and_filters)
                    for and_filters in filters
                ]
            )
        )

        def load_single_file_dask(
            f: str,
            columns: Optional[List[str]],
            filters: Optional[List[List[Tuple[str, str, Any]]]],
        ) -> dd.DataFrame:
            df = dd.read_parquet(
                f,
                dtype=self.convert_schema_for_dask(schema),
                columns=columns,
                filters=filters if apply_filters_on_load else None,
                engine="pyarrow",
                index=False,
            )

            if filters is not None and not apply_filters_on_load:
                df = self.apply_filters(df, filters, schema)

            df_columns = sorted(df.columns.values)
            df = df[df_columns]

            df = df.repartition(npartitions=1)
            df = df.reset_index(drop=True)

            return df

        ret = dd.multi.concat(
            [
                load_single_file_dask(f, columns=columns, filters=filters)
                for f in file_paths
            ]
        )

        return ret

    def save_parquet(
        self,
        file_path: str,
        dataframe: dd.DataFrame,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.

        Args:
            file_path: path to the file
            dataframe: dask data frame
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        dataframe.to_parquet(
            file_path,
            partition_on=partitions,
            schema=PyarrowCalculator.convert_schema_for_pyarrow(schema),
            engine="pyarrow",
        )

    def row_count(self, dataframe: dd.DataFrame) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        return len(dataframe.index)
