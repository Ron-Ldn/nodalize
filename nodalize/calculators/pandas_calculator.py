"""Pandas calculator."""
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from nodalize.calculators.calculator import Calculator


class PandasCalculator(Calculator[pd.DataFrame]):
    """Pandas calculator."""

    calculator_type = "pandas"

    def __init__(self, app_name: str) -> None:
        """
        Initialize calculator.

        Args:
            app_name: application name
        """
        Calculator.__init__(self, app_name)

    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Convert from pandas data frame to pandas data frame.

        Args:
            dataframe: pandas data frame

        Returns:
            pandas data frame
        """
        return dataframe

    def to_pandas(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Args:
            dataframe: pandas data frame

        Returns:
            pandas data frame
        """
        return dataframe

    def create_data_frame(
        self,
        values: Dict[str, List[Any]],
        types: Dict[str, type],
    ) -> pd.DataFrame:
        """
        Create data frame from column values.

        Args:
            values: column values
            types: column types

        Returns:
            data frame
        """
        return pd.DataFrame(values)

    def column_exists(self, dataframe: pd.DataFrame, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        return column_name in dataframe.columns

    def get_column(self, dataframe: pd.DataFrame, column_name: str) -> Any:
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
        dataframe: pd.DataFrame,
        column_name: str,
        value: Any,
        literal: bool = False,
        override: bool = True,
    ) -> pd.DataFrame:
        """
        Add column to data frame, with same default value on each rows.

        Args:
            dataframe: pandas data frame
            column_name: new column name
            value: value to assign to the new column
            literal: if True then will set the same value for the entire column, otherwise will consider it as a column
            override: if False, then will not try to replace an existing column

        Returns:
            pandas data frame
        """
        if override or not self.column_exists(dataframe, column_name):
            dataframe[column_name] = value

        return dataframe

    def filter_in_max_values(
        self, dataframe: pd.DataFrame, value_columns: List[str], key_columns: List[str]
    ) -> pd.DataFrame:
        """
        Filter on max values from the data frame.

        Value columns are typically date columns (InsertedDateTime).

        Args:
            dataframe: pandas data frame
            value_columns: list of columns for which we look for the maximum values
            key_columns: columns to group by on, before searching for the maximum values

        Returns:
            pandas data frame
        """
        key_columns = key_columns.copy()
        for c in key_columns:
            if c not in dataframe.columns:
                raise ValueError(f"Column not found in data frame: {c}")

        aggregation = dataframe
        for vc in value_columns:
            max_series = aggregation.groupby(key_columns)[vc].transform(max)
            aggregation = aggregation.loc[aggregation[vc] == max_series]
            key_columns.append(vc)
        return aggregation

    def drop_columns(
        self, dataframe: pd.DataFrame, columns: Iterable[str]
    ) -> pd.DataFrame:
        """
        Drop columns from data frame.

        Args:
            dataframe: pandas data frame
            columns: list of columns to remove

        Returns:
            pandas data frame
        """
        return dataframe.drop(list(columns), axis=1)

    def select_columns(
        self, dataframe: pd.DataFrame, columns: Iterable[str]
    ) -> pd.DataFrame:
        """
        Select columns in data frame.

        Args:
            dataframe: pandas data frame
            columns: list of columns to keep

        Returns:
            pandas data frame
        """
        return dataframe[list(columns)]

    def drop_duplicates(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Drop duplicates.

        Args:
            dataframe: pandas data frame

        Returns:
            pandas data frame
        """
        return dataframe.drop_duplicates()

    def concat(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
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
        return pd.concat(dataframes)

    def apply_filter(
        self, df: pd.DataFrame, filter: Tuple[str, str, Any], schema: Dict[str, type]
    ) -> pd.DataFrame:
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
        operator = filter[1].lower()
        right_op = filter[2]

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
        df: pd.DataFrame,
        filters: List[Tuple[str, str, Any]],
        schema: Dict[str, type],
    ) -> pd.DataFrame:
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
        df: pd.DataFrame,
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, type],
    ) -> pd.DataFrame:
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
            df[row_num_column] = np.arange(len(df))
            dfs = [self._apply_and_filters(df, filter, schema) for filter in filters]
            df = pd.concat(dfs)
            df = df.drop_duplicates()
            df = df.drop(columns=row_num_column)
            return df

    def left_join_data_frames(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: List[str],
    ) -> pd.DataFrame:
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
                left[cname] = left[cname].astype(right[cname].dtype)  # type: ignore

        df = pd.merge(
            left,
            right,
            how="left",
            on=on,
            suffixes=("", "_right"),
        )
        return df

    def rename_columns(
        self, dataframe: pd.DataFrame, names: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Rename columns in data frame.

        Args:
            dataframe: pandas data frame
            names: dictionary of column names - before: after

        Returns:
            pandas data frame
        """
        return dataframe.rename(columns=names)

    def extract_unique_column_values(
        self, column_name: str, dataframe: pd.DataFrame
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

    # @staticmethod
    # def convert_schema_for_pandas(schema: Dict[str, type]) -> Dict[str, np.dtype]:
    #     """
    #     Convert schema for pandas: replace native types with numpy types.

    #     Args:
    #         schema: dictionary of (name: str, type: type)

    #     Returns:
    #         dictionary of (name: str, type: np.dtype)
    #     """
    #     return {k: np.dtype(v) for k, v in schema.items()}

    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Load data frame from parquet files.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            pandas data frame
        """
        df = pd.concat(
            [
                pd.read_parquet(
                    f,
                    columns=columns,
                    filters=filters,
                    # dtype=convert_schema_for_pandas(schema)  - note: Pandas solely relies on file meta data to
                    # get the schema. It seems to work fine, even when there is a partitioning.
                )
                for f in file_paths
            ]
        )

        return df

    def save_parquet(
        self,
        file_path: str,
        dataframe: pd.DataFrame,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.

        Args:
            file_path: path to the file
            dataframe: pandas data frame
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        dataframe.to_parquet(file_path, partition_cols=partitions, engine="pyarrow")  # type: ignore

    def row_count(self, dataframe: pd.DataFrame) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        return len(dataframe)
