# Manage your own data store

Several data stores are supported already:
- Parquet files stored on file system
- Parquet files stored in AWS S3
- Delta Lake
- KDB
- Sqlite

It is possible to add a custom data store by implementing a new data manager. The new class must inherit from nodalize.data_management.data_manager.DataManager and define 2 functions: load_data_frame and save_updates.


```python
from typing import Any, Dict, List, Optional, Tuple
from nodalize.calculators.calculator import Calculator
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.data_manager import DataManager


class MyCustomDataManager(DataManager):   
    def load_data_frame(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data frame.

        Args:
            calculator: Calculator
            table_name: table name
            schema: table schema
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading
            batch_ids: optional list of batch ids to load, instead of the entire data set

        Returns:
             data frame (actual class type depends on type of calculator)
        """
        ...

    def save_updates(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        partitioning: Optional[List[str]],
        batch_id: int,
        dataframe: Any,
    ) -> None:
        """
        Save dataframe storing update data.

        Args:
            calculator: Calculator
            table_name: table name
            schema: table schema
            partitioning: optional list of partitioning columns
            batch_id: batch id used
            dataframe: data frame (actual class type depends on type of calculator)
        """
        ...
```

Note: the new class may not need to support all implementations of Calculator, but only those calculators actually used. For instance, if only Pandas is used for the process, then there is no need to support Polars or PySpark, etc.

To use the custom data manager, simply pass it as argument of the Coordinator.

```python
coordinator = Coordinator("test", my_custom_data_manager_instance)
```


The DataManager class contains a concrete function called "load_data_frame_with_lookback", which leverages "load_data_frame". Based on the type of data store, it might be relevant to override it in order to optimize the loading (see nodalize.data_management.kdb_data_manager). 

```python
    def load_data_frame_with_lookback(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        loading_date: date,
        lookback: int,
        group_lookback: bool = False,
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data frame, applying a lookback.

        Args:
            calculator: Calculator
            table_name: table name
            loading_date: date to load for, on which to apply the look back
            lookback: maximum possible age, in days, for the data to be loaded
            group_lookback: if True, then load all data from the latest date available
            schema: table schema
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading
            batch_ids: optional list of batch ids to load, instead of the entire data set

        Returns:
             data frame (actual class type depends on type of calculator)
        """
        ...
```

Finally, nodalize.data_management.database_data_manager.DatabaseDataManager is a specialization of DataManager, to handle data bases.

```python
from typing import Any, Callable, Dict, List, Optional, Tuple


from nodalize.calculators.calculator import Calculator
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.data_management.database_data_manager import DatabaseDataManager
from nodalize.tools.static_func_tools import generate_possible_name


class MyCustomDataManager(DatabaseDataManager):
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        ...

    def create_table(
        self,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        partitioning: Optional[List[str]],
    ) -> None:
        """
        Create table.

        Args:
            table_name: name of the table
            schema: schema of the table
            partitioning: optional list of partitioning columns
        """
        ...

    def check_table_schema(
        self, table_name: str, schema: Dict[str, Tuple[type, ColumnCategory]]
    ) -> Dict[str, type]:
        """
        Check schema of existing table. Fail if existing column type does not match.

        Args:
            table_name: name of the table
            schema: expected schema of the table

        Returns:
            columns missing from the schema
        """
        ...

    def load_data_from_database(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data frame.

        Args:
            calculator: Calculator
            table_name: table name
            schema: table schema
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading
            batch_ids: optional list of batch ids to load, instead of the entire data set

        Returns:
             data frame (actual class type depends on type of calculator)
        """
        ...

    def add_data_to_table(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        missing_columns: Dict[str, type],
        data: Any,
    ) -> None:
        """
        Add data to table.

        Args:
            calculator: Calculator
            table_name: name of the table
            schema: schema of the table
            missing_columns: columns from the table which are not in the data (to be defaulted)
            data: data frame
        """
        ...
```