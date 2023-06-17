# Create your own calculator

Several data frame based frameworks are supported:
- Pandas
- Dask
- PySpark
- PyArrow
- Polars

It is possible to add a custom one by implementing a new calculator. The new class must inherit from nodalize.calculators.calculator.Calculator and define the functions below.


```python
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast
import pandas as pd
from nodalize.calculators.calculator import Calculator
# from ... import T

class MyCustomCalculator(Calculator[T]):  # T is the type of data frame (ex: pandas.DataFrame)
    @property
    def calc_type(self) -> str:
        """
        Return unique name to identify the type of calculator.

        Returns:
            type name
        """
        ...

    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[T]:
        """
        Load data frame from parquet files.
        
        Only needed if the data store relies on parquet format.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            data frame
        """
        ...

    def save_parquet(
        self,
        file_path: str,
        dataframe: T,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.
        
        Only needed if the data store relies on parquet format.

        Args:
            file_path: path to the file
            dataframe: data frame
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        ...

    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> T:
        """
        Convert from pandas data frame to target data frame type.

        Useful for generic, automated tests, and for datastores that can only work with Pandas (ex: KDB using QPython3).

        Args:
            dataframe: pandas data frame

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        ...

    def to_pandas(self, dataframe: T) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Useful for generic, automated tests, and for datastores that can only work with Pandas (ex: KDB using QPython3).

        Args:
            dataframe: data frame (actual class type depends on type of calculator)

        Returns:
            pandas data frame
        """
        ...

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
        ...

    def column_exists(self, dataframe: T, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        ...

    def get_column(self, dataframe: T, column_name: str) -> Any:
        """
        Get column object.

        Args:
            dataframe: data frame
            column_name: column name to get

        Returns:
            column object
        """
        ...

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
        ...

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
        ...

    def drop_columns(self, dataframe: T, columns: Iterable[str]) -> T:
        """
        Drop columns from data frame.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            columns: list of columns to remove

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        ...

    def select_columns(self, dataframe: T, columns: Iterable[str]) -> T:
        """
        Select columns in data frame.

        Args:
            dataframe: data frame (actual class type depends on type of calculator)
            columns: list of columns to keep

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        ...

    def drop_duplicates(self, dataframe: T) -> T:
        """
        Drop duplicates.

        Args:
            dataframe:  (actual class type depends on type of calculator)

        Returns:
            data frame (actual class type depends on type of calculator)
        """
        ...

    def concat(self, dataframes: List[T]) -> T:
        """
        Concatenate data frames.

        Args:
            dataframes: list of data frames to concatenate

        Returns:
            data frame
        """
        ...
        
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
        ...

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
        ...

    def left_join_data_frames(
        self, left: T, right: T, on: List[str]
    ) -> T:
        """
        Left join 2 data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        ...

    def rename_columns(self, dataframe: T, names: Dict[str, str]) -> T:
        """
        Rename columns in data frame.

        Args:
            dataframe: data frame
            names: dictionary of column names - before: after

        Returns:
            data frame
        """
        ...

    def extract_unique_column_values(self, column_name: str, dataframe: T) -> Set[Any]:
        """
        Get existing values from a given column in the data frame.

        Args:
            column_name: name of the column
            dataframe: data frame

        Returns:
            set of values
        """
        ...

    def row_count(self, dataframe: T) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        ...
```