"""Data manager leveraging Delta Lake."""
import logging
import os
import sqlite3
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.data_management.database_data_manager import DatabaseDataManager
from nodalize.tools.dates import (
    datetime_series_to_unix,
    to_unix,
    unix_series_to_datetime,
)
from nodalize.tools.static_func_tools import generate_random_name


class SqliteDataManager(DatabaseDataManager):
    """Data manager leveraging Delta Lake."""

    def __init__(self, location: str) -> None:
        """
        Initialize.

        Args:
            location: location of data base
        """
        self._location = location

        folder = os.path.dirname(location)

        if not os.path.exists(folder):
            os.makedirs(folder)

    def _execute_query(self, query: str) -> List[Any]:
        """
        Execute query against Sqlite database.

        Args:
            query: SQL query

        Returns:
            list of recordsets
        """
        logging.info(f"Running Sqlite query: {query}")
        with sqlite3.connect(self._location) as conn:
            cur = conn.cursor()
            return cur.execute(query).fetchall()

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        query = (
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        ret = self._execute_query(query)
        if len(ret) == 0:
            return False
        elif len(ret) == 1 and len(ret[0]) == 1 and ret[0][0] == table_name:
            return True
        else:
            raise AssertionError(f"Unexpected response from sqlite: {ret}")

    def _convert_type_for_sqlite(self, t: type) -> str:
        """
        Convert from Python type to sqlite type as str.

        Args:
            t: Python type

        Returns:
            sqlite type declaration
        """
        if t == int:
            return "INTEGER"
        elif t == float:
            return "REAL"
        elif t == str:
            return "TEXT"
        elif t == bool:
            return "INTEGER"
        elif t == date:
            return "INTEGER"  # unix format
        elif t == datetime:
            return "INTEGER"  # unix format
        elif t == Symbol:
            return "TEXT"
        else:
            raise NotImplementedError(f"Type not supported for Sqlite: {t}")

    def _convert_from_sqlite_type_to_python(self, t: str) -> type:
        """
        Convert sqlite type to Python type.

        Args:
            t: sqlite type as str

        Returns:
            Python type
        """
        if t == "INTEGER":
            return int
        elif t == "REAL":
            return float
        elif t == "TEXT":
            return str
        else:
            raise NotImplementedError(f"Type not supported from Sqlite: {t}")

    def _create_column_declaration(self, column_name: str, t: type) -> str:
        """
        Create column declaration statement for CREATE TABLE.

        Args:
            column_name: column header
            t: column type

        Returns:
            SQL declaration
        """
        tp = self._convert_type_for_sqlite(t)
        return f"[{column_name}] {tp}"

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
        column_declarations = [
            self._create_column_declaration(col, tp) for col, (tp, _) in schema.items()
        ]
        column_declarations_sql = ", ".join(column_declarations)
        query = f"CREATE TABLE [{table_name}]({column_declarations_sql})"
        self._execute_query(query)

        if partitioning is not None and len(partitioning) > 0:
            partition_cols = ",".join(partitioning)
            index_name = generate_random_name(10)
            query = f"CREATE INDEX [{index_name}] ON [{table_name}]({partition_cols})"
            self._execute_query(query)

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
        ret = self._execute_query(f"PRAGMA TABLE_INFO('{table_name}')")

        new_columns = set(schema.keys())
        missing_columns = {}
        for record in ret:
            column = record[1]
            tp = record[2]

            expected_col_schema = schema.get(column)
            if expected_col_schema is None:
                missing_columns[column] = self._convert_from_sqlite_type_to_python(tp)
            else:
                new_columns.remove(column)

                expected_tp = self._convert_type_for_sqlite(expected_col_schema[0])
                if expected_tp != tp:
                    raise AssertionError(
                        f"Type mismatch found for column {table_name}.{column}: sqlite={tp} whilst schema={expected_tp}"
                    )

        if len(new_columns) > 0:
            raise AssertionError(
                f"Columns in schema are missing in {table_name}: {list(new_columns)}"
            )

        return missing_columns

    @staticmethod
    def _convert_value(t: type, v: Any) -> str:
        """
        Convert value to sqlite.

        Args:
            t: value type
            v: value

        Returns:
            KDB value as string
        """
        if v is None:
            return "NULL"

        if t == int:
            if np.isnan(v):
                return "NULL"
            else:
                return str(v)
        elif t == float:
            if np.isnan(v):
                return "NULL"
            else:
                return str(v)
        elif t in [str, Symbol]:
            return f"'{v}'"
        elif t == bool:
            return "1" if v else "0"
        elif t in [date, datetime]:
            return str(to_unix(v))
        else:
            raise NotImplementedError(f"Type not supported for sqlite: {t}")

    def _convert_filter(
        self,
        filter: Tuple[str, str, Any],
        schema: Dict[str, Tuple[type, ColumnCategory]],
        table_prefix: str,
    ) -> str:
        """
        Convert filter to SQL query format.

        Args:
            filter: tuple (left operand, operator, right operand)
            schema: table schema
            table_prefix: prefix of the table in the query

        Returns:
            SQL filter
        """
        column = filter[0]
        if column not in schema.keys():
            raise AssertionError(f"Filter applies to unknown column: {column}")

        operator = filter[1].upper()
        if operator == "!=":
            operator = "<>"

        tp = schema[column][0]
        value = filter[2]

        if operator in ["IN", "NOT IN"]:
            if not isinstance(value, list):
                raise AssertionError(
                    "Filter operator 'in' must be provided a list as value"
                )
            else:
                converted_values = [self._convert_value(tp, v) for v in value]

                value_list = ",".join(converted_values)
                return f"{table_prefix}.{column} {operator} ({value_list})"
        else:
            converted_value = self._convert_value(tp, value)
            return f"{table_prefix}.{column} {operator} {converted_value}"

    def _convert_and_filters(
        self,
        filters: List[Tuple[str, str, Any]],
        schema: Dict[str, Tuple[type, ColumnCategory]],
        table_prefix: str,
    ) -> str:
        """
        Convert "AND" list of filters to SQL query format.

        Args:
            filters: list of filters
            schema: table schema
            table_prefix: prefix of the table in the query

        Returns:
            SQL filter
        """
        return " AND ".join(
            f"({self._convert_filter(f, schema, table_prefix)})" for f in filters
        )

    def _convert_or_filters(
        self,
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, Tuple[type, ColumnCategory]],
        table_prefix: str,
    ) -> str:
        """
        Convert "OR" list of filters to SQL query format.

        Args:
            filters: list of list of filters (super list is OR and sub list is AND)
            schema: table schema
            table_prefix: prefix of the table in the query

        Returns:
            SQL query filter
        """
        if all(len(f) == 0 for f in filters):
            return ""
        else:
            return " OR ".join(
                f"({self._convert_and_filters(f, schema, table_prefix)})"
                for f in filters
            )

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
        key_columns = [
            col
            for col, (_, cat) in schema.items()
            if cat in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
        ]
        key_columns_list = ",".join(key_columns)

        if columns is None:
            columns = list(schema.keys())
        else:
            columns = columns[:]

        column_list2 = ", ".join([f"A.{col}" for col in columns])

        for col in key_columns:
            if col not in columns:
                columns.append(col)
        column_list = ", ".join(columns)

        if batch_ids is not None:
            if filters is None:
                filters = [[(column_names.BATCH_ID, "in", batch_ids)]]
            else:
                for f in filters:
                    f.append((column_names.BATCH_ID, "in", batch_ids))

        if filters is not None:
            sql_filter = self._convert_or_filters(filters, schema, "A")
            if sql_filter != "":
                sql_filter = f"WHERE {sql_filter}"
        else:
            sql_filter = ""

        max_datetime_col_name = generate_random_name(10)

        joins = [f"A.{col}=B.{col}" for col in key_columns]
        joins.append(f"A.{column_names.INSERTED_DATETIME}=B.{max_datetime_col_name}")
        joins_list = " AND ".join(joins)

        cte_name = generate_random_name(10)

        query = f"""
WITH [{cte_name}] AS (
    SELECT {column_list}, MAX({column_names.INSERTED_DATETIME}) AS {max_datetime_col_name}
    FROM [{table_name}]
    GROUP BY {key_columns_list}
)
SELECT {column_list2}
FROM [{table_name}] A
INNER JOIN [{cte_name}] B on {joins_list}
{sql_filter}
"""

        logging.info(f"Running Sqlite query: {query}")
        with sqlite3.connect(self._location) as conn:
            pdf = pd.read_sql_query(query, conn)

        for col, (tp, _) in schema.items():
            if col not in pdf.columns:
                continue

            if tp in [date, datetime]:
                pdf[col] = unix_series_to_datetime(pdf[col])
            elif tp == float:
                pdf[col] = pdf[col].astype(np.float64)
            elif tp == int:
                pdf[col] = pdf[col].astype(np.int64)
            elif tp == bool:
                pdf[col] = pdf[col].astype(np.bool_)

        return calculator.from_pandas(pdf)

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
        with sqlite3.connect(self._location) as conn:
            pdf = pd.read_sql_query(query, conn)

        for col, tp in schema.items():
            if col not in pdf.columns:
                continue

            if tp in [date, datetime]:
                pdf[col] = unix_series_to_datetime(pdf[col])
            elif tp == float:
                pdf[col] = pdf[col].astype(np.float64)
            elif tp == int:
                pdf[col] = pdf[col].astype(np.int64)
            elif tp == bool:
                pdf[col] = pdf[col].astype(np.bool_)

        return calculator.from_pandas(pdf)

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
        pdf = calculator.to_pandas(data)
        dtCols = [
            col
            for col, (tp, _) in schema.items()
            if tp in [date, datetime] and col in pdf.columns
        ]
        if any(dtCols):
            pdf = pdf.copy()
            for dtCol in dtCols:
                pdf[dtCol] = datetime_series_to_unix(pdf[dtCol])

        temp_table_name = generate_random_name(10)

        columns = list(schema.keys())
        columns_list = ", ".join(columns)

        insert_query = f"""
INSERT INTO [{table_name}] ({columns_list})
SELECT {columns_list}
FROM [{temp_table_name}]
"""

        self._execute_query(f"DROP TABLE IF EXISTS [{temp_table_name}]")

        try:
            with sqlite3.connect(self._location) as conn:
                pdf.to_sql(temp_table_name, conn, if_exists="replace")
                conn.cursor().execute(insert_query)
        finally:
            self._execute_query(f"DROP TABLE IF EXISTS [{temp_table_name}]")
