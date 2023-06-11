"""Data manager leveraging Delta Lake."""
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, PyDeltaTableError, write_deltalake

from nodalize.calculators.calculator import Calculator
from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.database_data_manager import DatabaseDataManager


class DeltaLakeDataManager(DatabaseDataManager):
    """Data manager leveraging Delta Lake."""

    def __init__(self, location: str) -> None:
        """
        Initialize.

        Args:
            location: location of Delta Lake.
        """
        self._location = location
        self._table_partitionings = {}  # type: Dict[str, Optional[List[str]]]

    def _get_table_path(self, table_name: str) -> str:
        """
        Get path to delta table from table name.

        Args:
            table_name: name of the table

        Returns:
            path to delta table
        """
        return os.path.join(self._location, table_name)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        table_path = self._get_table_path(table_name)

        # Note: the workflow below is ugly but I couldn't find any API to check
        # the existence of a table without using Spark.
        # An alternative would be to manually check the existence of the file, but
        # the treatment would be dependent on the location (file system, S3, etc.).
        try:
            _ = DeltaTable(table_path)
            return True
        except PyDeltaTableError:
            return False

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
        # Table will be created when saving data. For now just store meta data in cache.
        self._table_partitionings[table_name] = partitioning

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
        table_path = self._get_table_path(table_name)
        table = DeltaTable(table_path)
        existing_schema = PyarrowCalculator.convert_schema_from_pyarrow(
            table.pyarrow_schema()
        )

        removed = {}
        found = []
        for column, delta_type in existing_schema.items():
            found.append(column)
            schema_type = schema.get(column)
            if schema_type is None:
                removed[column] = delta_type
            elif delta_type != schema_type[0]:
                raise AssertionError(
                    f"Type mismatch found for column {table_path}.{column}: delta={delta_type} "
                    f"whilst schema={schema_type[0]}"
                )

        added = [k for k in schema.keys() if k not in found]
        if any(added):
            raise AssertionError(
                f"Columns in schema are missing in {table_path}: {added}"
            )

        return removed

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
        if batch_ids is not None:
            if filters is None:
                filters = [[(column_names.BATCH_ID, "in", batch_ids)]]
            else:
                for sub_filter in filters:
                    sub_filter.append((column_names.BATCH_ID, "in", batch_ids))

        keys = [
            c
            for c, t in schema.items()
            if t[1] in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
        ]
        table_path = self._get_table_path(table_name)

        if calculator.calc_type == "spark":
            df = calculator.load_delta_table(table_path)  # type: ignore
            df = calculator.filter_in_max_values(
                df, [column_names.INSERTED_DATETIME], keys
            )

            if filters is not None:
                df = calculator.apply_filters(
                    df, filters, {col: tp for col, (tp, _) in schema.items()}
                )

            if columns is not None:
                df = calculator.select_columns(df, columns)

            return df
        else:
            partitions = self._table_partitionings.get(table_name)

            # Note: we cannot filter out the key columns until the call to filter_in_max_values
            if columns is not None:
                column_pre_filter = columns + [column_names.INSERTED_DATETIME] + keys

                if batch_ids is not None:
                    column_pre_filter.append(column_names.BATCH_ID)

                column_pre_filter = list(set(column_pre_filter))
            else:
                column_pre_filter = None

            # If the filters contain non-key or non-parameter columns, then they must be applied after
            # the search for latest version. Moreover, we can only apply filter on partitioned
            # columns when loading.
            if filters is None:
                load_filters = (
                    None
                )  # type: Optional[List[Optional[List[Tuple[str, str, Any]]]]]
                post_filters = (
                    None
                )  # type: Optional[List[Optional[List[Tuple[str, str, Any]]]]]
            elif partitions is None:
                load_filters = None
                post_filters = filters  # type: ignore
            else:
                load_filters = []
                post_filters = []

                for inner_filters in filters:
                    load_inner_filters = []
                    post_inner_filters = []
                    for condition in inner_filters:
                        if (
                            condition[0] in keys
                            and condition[0] in partitions
                            and condition[1].lower() in ["=", "!=", "in", "not in"]
                        ):
                            # DeltaLake requires right operand to be a string
                            if condition[1].lower() in ["in", "not in"]:
                                condition = (
                                    condition[0],
                                    condition[1],
                                    [str(v) for v in condition[2]],
                                )
                            else:
                                condition = (
                                    condition[0],
                                    condition[1],
                                    str(condition[2]),
                                )
                            load_inner_filters.append(condition)
                        else:
                            post_inner_filters.append(condition)
                    load_filters.append(
                        load_inner_filters if len(load_inner_filters) > 0 else None
                    )
                    post_filters.append(
                        post_inner_filters if len(post_inner_filters) > 0 else None
                    )

                if all(f is None for f in load_filters):
                    load_filters = None
                    post_filters = filters  # type: ignore
                elif all(f is None for f in post_filters):
                    load_filters = filters  # type: ignore
                    post_filters = None

            def load_df(pre_f, post_f):
                table = DeltaTable(table_path)

                if calculator.calc_type == "pyarrow":
                    df = table.to_pyarrow_table(pre_f, column_pre_filter)
                else:
                    pandas_df = table.to_pandas(pre_f, column_pre_filter)
                    df = calculator.from_pandas(pandas_df)

                if df is not None:
                    df = calculator.filter_in_max_values(
                        df,
                        [column_names.INSERTED_DATETIME],
                        [k for k in keys if k != column_names.INSERTED_DATETIME],
                    )
                    if post_f is not None:
                        df = calculator.apply_filters(
                            df, post_f, {col: tp for col, (tp, _) in schema.items()}
                        )

                return df

            if load_filters is None:
                df = load_df(None, post_filters)
            else:
                dfs = []
                for i, load_filter in enumerate(load_filters):
                    post_filter = (
                        [post_filters[i]]
                        if post_filters is not None and post_filters[i] is not None
                        else None
                    )
                    df = load_df(load_filter, post_filter)
                    if df is not None:
                        dfs.append(df)

                if len(dfs) == 0:
                    df = None
                else:
                    df = calculator.concat(dfs)
                    df = calculator.drop_duplicates(df)

            # Filter columns
            if columns is not None and df is not None:
                df = calculator.select_columns(df, columns)

            return df

    def load_data_frame_from_query(
        self,
        calculator: Calculator,
        query: str,
        schema: Dict[str, type],
    ) -> Any:
        """
        Load data frame based on custom query.

        Args:
            calculator: Calculator
            query: custom query
            schema: schema of the expected output

        Returns:
            Any: custom query
        """
        raise NotImplementedError("Custom query not available for file based loader")

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
        table_path = self._get_table_path(table_name)

        if calculator.calc_type == "spark":
            write_func = data.write.format("delta").mode("append")

            partitioning = self._table_partitionings.get(table_name)
            if partitioning is not None:
                write_func = write_func.partitionBy(*partitioning)

            write_func.save(table_path)
        else:
            if calculator.calc_type == "pyarrow":
                table = data
            else:
                table = pa.Table.from_pandas(calculator.to_pandas(data))

            pyarrowCalculator = PyarrowCalculator(calculator.app_name)
            table = pyarrowCalculator.select_columns(
                table, list(schema.keys()) + list(missing_columns.keys())
            )

            for col, (tp, _) in schema.items():
                if tp == datetime:
                    new_col = pc.cast(table[col], pa.timestamp("us"))
                    table = pyarrowCalculator.add_column(
                        table, col, new_col, False, True
                    )
                elif tp == date:
                    new_col = pc.cast(table[col], pa.date32())
                    table = pyarrowCalculator.add_column(
                        table, col, new_col, False, True
                    )

            converted_types = {}
            for col in table.column_names:
                tuple = schema.get(col)
                if tuple is not None:
                    pyarrow_type = pyarrowCalculator.convert_type_for_pyarrow(tuple[0])
                    if pyarrow_type == pa.timestamp("ns"):
                        pyarrow_type = pa.timestamp("us")
                    elif pyarrow_type == pa.date64():
                        pyarrow_type = pa.date32()
                    converted_types[col] = pyarrow_type
            pyarrow_schema = pa.schema(converted_types)

            write_deltalake(
                table_path,
                table,
                mode="append",
                schema=pyarrow_schema,
                partition_by=self._table_partitionings.get(table_name),
            )
