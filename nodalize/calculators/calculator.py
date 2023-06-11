"""Base class for calculators."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Iterable, List, Optional, Set, Tuple, TypeVar

import pandas as pd

from nodalize.tools.static_func_tools import generate_possible_name

T = TypeVar("T")


class Calculator(ABC, Generic[T]):
    """Base class for calculators."""

    def __init__(self, app_name: str) -> None:
        """
        Initialize base class for calculators.

        Args:
            app_name: application name
        """
        self.app_name = app_name

    @property
    def calc_type(self) -> str:
        """
        Return unique name to identify the type of calculator.

        Returns:
            type name
        """
        if hasattr(self, "calculator_type"):
            return getattr(self, "calculator_type")
        else:
            raise NotImplementedError(
                f"Field missing in class {self.__class__}: calculator_type"
            )

    @abstractmethod
    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[T]:
        """
        Load data frame from parquet files.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            data frame
        """
        raise NotImplementedError

    @abstractmethod
    def save_parquet(
        self,
        file_path: str,
        dataframe: T,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.

        Args:
            file_path: path to the file
            dataframe: data frame
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        raise NotImplementedError

    @abstractmethod
    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> T:
        """
        Convert from pandas data frame to target data frame type.

        Args:
            dataframe: pandas data frame

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def to_pandas(self, dataframe: T) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)

        Returns:
            pandas data frame
        """
        raise NotImplementedError

    @abstractmethod
    def create_data_frame(
        self,
        values: Dict[str, List[Any]],
        types: Dict[str, type],
    ) -> T:
        """
        Create data frame from column values.

        Args:
            values: column values
            types: column types

        Returns:
            data frame
        """
        raise NotImplementedError

    @abstractmethod
    def column_exists(self, dataframe: T, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        raise NotImplementedError

    @abstractmethod
    def get_column(self, dataframe: T, column_name: str) -> Any:
        """
        Get column object.

        Args:
            dataframe: data frame
            column_name: column name to get

        Returns:
            column object
        """
        raise NotImplementedError

    @abstractmethod
    def add_column(
        self,
        dataframe: T,
        column_name: str,
        value: Any,
        literal: bool = False,
        override: bool = True,
    ) -> T:
        """
        Add column to data frame, with same default value on each rows.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            column_name: new column name
            value: value to assign to the new column
            literal: if True then will set the same value for the entire column, otherwise will consider it as a column
            override: if False, then will not try to replace an existing column

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def filter_in_max_values(
        self, dataframe: T, value_columns: List[str], key_columns: List[str]
    ) -> T:
        """
        Filter on max values from the data frame.

        Value columns are typically date columns (InsertedDateTime).

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            value_columns: list of columns for which we look for the maximum values
            key_columns: columns to group by on, before searching for the maximum values

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def drop_columns(self, dataframe: T, columns: Iterable[str]) -> T:
        """
        Drop columns from data frame.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            columns: list of columns to remove

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def select_columns(self, dataframe: T, columns: Iterable[str]) -> T:
        """
        Select columns in data frame.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            columns: list of columns to keep

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def drop_duplicates(self, dataframe: T) -> T:
        """
        Drop duplicates.

        Args:
            dataframe:  (actual class type depends on type of calculator)

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        raise NotImplementedError

    @abstractmethod
    def concat(self, dataframes: List[T]) -> T:
        """
        Concatenate data frames.

        Args:
            dataframes: list of data frames to concatenate

        Returns:
            data frame
        """
        raise NotImplementedError

    @abstractmethod
    def apply_filter(
        self, df: T, filter: Tuple[str, str, Any], schema: Dict[str, type]
    ) -> T:
        """
        Apply filter to data frame and return new instance.

        Args:
            df: data frame
            filter: filter
            schema: schema

        Returns:
            new data frame
        """
        raise NotImplementedError

    @abstractmethod
    def apply_filters(
        self, df: T, filters: List[List[Tuple[str, str, Any]]], schema: Dict[str, type]
    ) -> T:
        """
        Apply filters to data frame and return new instance.

        Args:
            df: data frame
            filters: filters
            schema: schema

        Returns:
            new data frame
        """
        raise NotImplementedError

    @abstractmethod
    def left_join_data_frames(self, left: T, right: T, on: List[str]) -> T:
        """
        Join 2 data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        raise NotImplementedError

    @abstractmethod
    def rename_columns(self, dataframe: T, names: Dict[str, str]) -> T:
        """
        Rename columns in data frame.

        Args:
            dataframe: data frame
            names: dictionary of column names - before: after

        Returns:
            data frame
        """
        raise NotImplementedError

    @abstractmethod
    def extract_unique_column_values(self, column_name: str, dataframe: T) -> Set[Any]:
        """
        Get existing values from a given column in the data frame.

        Args:
            column_name: name of the column
            dataframe: data frame

        Returns:
            set of values
        """
        raise NotImplementedError

    def row_count(self, dataframe: T) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        raise NotImplementedError

    def generate_temporary_column_name(self, dataframe: T, length: int = 10) -> str:
        """
        Generate name for temporary column to add to the data frame.

        Args:
            dataframe: data frame
            length: length of the string generated

        Returns:
            column name
        """
        return generate_possible_name(
            lambda n: not self.column_exists(dataframe, n), length
        )
