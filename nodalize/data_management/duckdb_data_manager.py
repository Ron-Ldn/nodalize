"""Data manager leveraging KDB."""
import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import numpy as np

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.data_management.database_data_manager import DatabaseDataManager
from nodalize.tools.static_func_tools import generate_random_name


class DuckdbDataManager(DatabaseDataManager):
    """Data manager leveraging Duckdb."""

    def __init__(self, location: str, schema: Optional[str] = None) -> None:
        """
        Initialize.

        Args:
            location: location of data base
            schema: optional schema to use
        """
        self._location = location

        folder = os.path.dirname(location)

        if not os.path.exists(folder):
            os.makedirs(folder)

        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self._schema_prefix = "" if schema is None else f"{schema}."

    def execute_query(
        self, query: str, conn: Optional[duckdb.DuckDBPyConnection] = None
    ) -> Any:
        """
        Execute query.

        Args:
            query: query to execute

        Returns:
            any output
        """
        logging.info(f"Running  query: {query}")

        if conn is not None:
            return conn.sql(query)
        else:
            with duckdb.connect(self._location) as conn:
                return conn.sql(query)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        try:
            self.execute_query(f"DESCRIBE {self._schema_prefix}{table_name}")
        except duckdb.CatalogException:
            return False
        else:
            return True

    def _convert_type_to_duckdb(self, t: type) -> str:
        """
        Convert python type to duckdb declaration.

        Args:
            t: python type

        Returns:
            kdb type as string
        """
        if t == int:
            return "BIGINT"
        elif t == float:
            return "DOUBLE"
        elif t == str:
            return "VARCHAR"
        elif t == bool:
            return "BOOLEAN"
        elif t == date:
            return "DATE"
        elif t == datetime:
            return "TIMESTAMP"
        elif t == Symbol:
            return "VARCHAR"
        else:
            raise NotImplementedError(f"Type not supported for duckdb: {t}")

    def _convert_type_from_duckdb(self, t: str) -> type:
        """
        Convert python type to duckdb declaration.

        Args:
            t: python type

        Returns:
            kdb type as string
        """
        if t == "BIGINT":
            return int
        elif t == "DOUBLE":
            return float
        elif t == "VARCHAR":
            return str
        elif t == "BOOLEAN":
            return bool
        elif t == "DATE":
            return date
        elif t == "TIMESTAMP":
            return datetime
        else:
            raise NotImplementedError(f"Type not supported from duckdb: {t}")

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
        columns = [
            f"{c} {self._convert_type_to_duckdb(tp)}" for c, (tp, cat) in schema.items()
        ]
        columns_decl = ",".join(columns)

        if any(_ for _, (_, cat) in schema.items() if cat in [ColumnCategory.KEY]):
            keys = [
                c
                for c, (_, cat) in schema.items()
                if cat in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
            ] + [column_names.INSERTED_DATETIME]
            pk_decl = f", PRIMARY KEY({','.join(keys)})"
        else:
            pk_decl = ""

        query = (
            f"CREATE TABLE {self._schema_prefix}{table_name}({columns_decl}{pk_decl});"
        )
        self.execute_query(query)

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
        columns_found = {}

        with duckdb.connect(self._location) as conn:
            for row_desc in self.execute_query(
                f"DESCRIBE {self._schema_prefix}{table_name}", conn
            ).fetchall():
                col_name = row_desc[0]
                tp = row_desc[1]
                pk = row_desc[3] == "PRI"

                python_type = self._convert_type_from_duckdb(tp)
                columns_found[col_name] = python_type

                expected_schema = schema.get(col_name)

                if expected_schema is not None:
                    if tp != self._convert_type_to_duckdb(expected_schema[0]):
                        raise AssertionError(
                            f"Type mismatch found for column {table_name}.{col_name}: "
                            f"duckdb={python_type} whilst schema={expected_schema[0]}"
                        )
                    if (
                        pk
                        and expected_schema[1]
                        not in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
                        and col_name != column_names.INSERTED_DATETIME
                    ):
                        raise AssertionError(
                            f"Type mismatch found for column {table_name}.{col_name}: "
                            "primary key in duckdb but not in schema"
                        )
                    elif not pk and expected_schema[1] == ColumnCategory.KEY:
                        raise AssertionError(
                            f"Type mismatch found for column {table_name}.{col_name}: "
                            "primary key in schema but not in duckdb"
                        )

        added = [k for k in schema.keys() if k not in columns_found.keys()]
        if any(added):
            raise AssertionError(
                f"Columns in schema are missing in {table_name}: {added}"
            )

        return {k: tp for k, tp in columns_found.items() if k not in schema.keys()}

    @staticmethod
    def _convert_value(t: type, v: Any) -> str:
        """
        Convert value to sqlite.

        Args:
            t: value type
            v: value

        Returns:
            sqlite value as string
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
        elif t == date:
            return f"'{v.strftime('%Y-%m-%d')}'"
        elif t == datetime:
            return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            raise NotImplementedError(f"Type not supported for duckdb: {t}")

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

        column_list = ", ".join([f"A.{col}" for col in columns])

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

        max_datetime_col_name = "t" + generate_random_name(10)

        joins = [f"A.{col}=B.{col}" for col in key_columns]
        joins.append(f"A.{column_names.INSERTED_DATETIME}=B.{max_datetime_col_name}")
        joins_list = " AND ".join(joins)

        cte_name = "t" + generate_random_name(10)

        query = f"""
WITH {cte_name} AS (
    SELECT {key_columns_list}, MAX({column_names.INSERTED_DATETIME}) AS {max_datetime_col_name}
    FROM {self._schema_prefix}{table_name}
    GROUP BY {key_columns_list}
)
SELECT {column_list}
FROM {self._schema_prefix}{table_name} A
INNER JOIN {cte_name} B on {joins_list}
{sql_filter}
"""

        with duckdb.connect(self._location) as conn:
            ret = self.execute_query(query, conn)

            if calculator.calc_type == "pandas":
                return ret.df()
            elif calculator.calc_type == "polars":
                return ret.pl()
            elif calculator.calc_type == "pyarrow":
                return ret.arrow()
            else:
                pdf = ret.df()

                for col, (tp, _) in schema.items():
                    if col not in pdf.columns:
                        continue

                    if tp in [date, datetime]:
                        pdf[col] = pdf[col].dt.to_pydatetime()

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
        ret = self.execute_query(query)

        if calculator.calc_type == "pandas":
            return ret.df()
        elif calculator.calc_type == "polars":
            return ret.pl()
        elif calculator.calc_type == "pyarrow":
            return ret.arrow()
        else:
            pandas_df = ret.df()
            return calculator.from_pandas(pandas_df)

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
        if calculator.calc_type in ["pandas", "pyarrow"]:
            df_name = "data"
        else:
            pdf = calculator.to_pandas(data)  # noqa: F841
            df_name = "pdf"

        columns = list(schema.keys())
        columns_list = ", ".join(columns)

        insert_query = f"""
INSERT INTO {self._schema_prefix}{table_name} ({columns_list})
SELECT {columns_list}
FROM {df_name}
"""

        self.execute_query(insert_query)
