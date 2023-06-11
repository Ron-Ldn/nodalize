"""Data manager leveraging KDB."""
import logging
import threading
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from qpython.qconnection import QConnection

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.constants.custom_types import Symbol
from nodalize.data_management.database_data_manager import DatabaseDataManager
from nodalize.tools.static_func_tools import generate_possible_name


class KdbDataManager(DatabaseDataManager):
    """Data manager leveraging KDB."""

    def __init__(
        self,
        namespace: Optional[str],
        host: str,
        port: int,
        credentials_getter: Callable,
    ) -> None:
        """
        Initialize.

        Args:
            namespace: namespace to use in KDB
            host: KDB host name
            port: port number
            credentials_getter: function to return username and password
        """
        self._namespace = namespace
        self._host = host
        self._port = port
        self._credentials_getter = credentials_getter
        self._connection = None  # type: Optional[QConnection]
        self._pandas_connection = None  # type: Optional[QConnection]
        self._lock = threading.Lock()

    def get_connection(self, read_only: bool, use_pandas: bool) -> QConnection:
        """
        Get connection object to KDB.

        Can be overridden to manage pool of ports, for instance.

        Args:
            read_only: flag to get read-only connection.
            use_pandas: flag to return results as pandas data frame

        Returns:
            opened connection object
        """
        if use_pandas:
            if self._pandas_connection is None:
                with self._lock:
                    if self._pandas_connection is None:
                        user, pwd = self._credentials_getter()
                        self._pandas_connection = QConnection(
                            self._host, self._port, user, pwd, pandas=True
                        )
                        self._pandas_connection.open()
            return self._pandas_connection
        else:
            if self._connection is None:
                with self._lock:
                    if self._connection is None:
                        user, pwd = self._credentials_getter()
                        self._connection = QConnection(
                            self._host, self._port, user, pwd
                        )
                        self._connection.open()
            return self._connection

    def execute_query(
        self, query: str, read_only: bool, use_pandas: bool = False
    ) -> Any:
        """
        Execute query.

        Args:
            query: query to execute
            read_only: flag for read_only queries
            use_pandas: flag to return results as pandas data frame

        Returns:
            any output
        """
        logging.info(f"Running {'read-only' if read_only else ''} query: {query}")
        conn = self.get_connection(read_only, use_pandas)

        return conn(query)

    def execute_query_with_args(self, query: str, *args: Any) -> Any:
        """
        Execute query.

        Args:
            query: query to execute
            read_only: flag for read_only queries
            use_pandas: flag to return results as pandas data frame
            args: extra arguments to pass to KDQ query

        Returns:
            any output
        """
        logging.info(f"Running query: {query}")
        conn = self.get_connection(False, False)

        if len(args) == 0:
            return conn(query)
        else:
            return conn(query, *args)

    @staticmethod
    def _convert_type_to_kdb_creation(t: type) -> str:
        """
        Convert python type to KDB declaration.

        Args:
            t: python type

        Returns:
            kdb type as string
        """
        if t == int:
            return "`long$()"
        elif t == float:
            return "`float$()"
        elif t == str:
            return "()"
        elif t == bool:
            return "`boolean$()"
        elif t == date:
            return "`date$()"
        elif t == datetime:
            return "`datetime$()"
        elif t == Symbol:
            return "`symbol$()"
        else:
            raise NotImplementedError(f"Type not supported for KDB: {t}")

    @staticmethod
    def _convert_type_to_kdb_csv(t: type) -> str:
        """
        Convert python type to KDB declaration.

        Args:
            t: python type

        Returns:
            kdb type as string
        """
        if t == int:
            return "J"
        elif t == float:
            return "F"
        elif t == str:
            return "C"
        elif t == bool:
            return "B"
        elif t == date:
            return "D"
        elif t == datetime:
            return "Z"
        elif t == Symbol:
            return "S"
        else:
            raise NotImplementedError(f"Type not supported for KDB: {t}")

    @staticmethod
    def _convert_from_kdb_type_to_python(t: str) -> type:
        """
        Convert from KDB table to python native type.

        Args:
            t: KDB type as string

        Returns:
            python type
        """
        if t == "s":
            return Symbol
        elif t == "b":
            return bool
        elif t in ["h", "i", "j"]:
            return int
        elif t == "f":
            return float
        elif t in [" ", "C"]:
            return str
        elif t == "d":
            return date
        elif t == "z":
            return datetime
        else:
            raise NotImplementedError(f"Type not supported from KDB: {t}")

    @staticmethod
    def _convert_from_python_type_to_kdb(t: type) -> str:
        """
        Convert from python native type to KDB.

        Args:
            t: python type

        Returns:
            KDB type as string
        """
        if t == Symbol:
            return "s"
        elif t == bool:
            return "b"
        elif t == int:
            return "j"
        elif t == float:
            return "f"
        elif t == str:
            return "C"
        elif t == date:
            return "d"
        elif t == datetime:
            return "z"
        else:
            raise NotImplementedError(f"Type not supported from KDB: {t}")

    @staticmethod
    def _get_null_value(t: type) -> str:
        """
        Get KDB null value for a given type.

        Args:
            t: python type

        Returns:
            value as string
        """
        if t == int:
            return "0N"
        elif t == float:
            return "0n"
        elif t == str:
            return '""'
        elif t == bool:
            return "0b"  # Note: KDB does not differentiate between NULL and FALSE
        elif t == date:
            return "0Nd"
        elif t == datetime:
            return "0Nz"
        elif t == Symbol:
            return "`"
        else:
            raise NotImplementedError(f"Type not supported for KDB: {t}")

    @staticmethod
    def _convert_value(t: type, v: Any) -> str:
        """
        Convert value to KDB.

        Args:
            t: value type
            v: value

        Returns:
            KDB value as string
        """
        if v is None:
            return KdbDataManager._get_null_value(t)

        if t == int:
            if np.isnan(v):
                return KdbDataManager._get_null_value(t)
            else:
                return str(v)
        elif t == float:
            if np.isnan(v):
                return KdbDataManager._get_null_value(t)
            else:
                return str(v)
        elif t == str:
            if len(v) == 1:
                raise AssertionError("Cannot convert single char to string")
            return f'"{v}"'
        elif t == bool:
            return "1b" if v else "0b"
        elif t == date:
            return v.strftime("%Y.%m.%d")
        elif t == datetime:
            return v.strftime("%Y.%m.%dT%H:%M:%S")
        elif t == Symbol:
            return f"`{v}"
        else:
            raise NotImplementedError(f"Type not supported for KDB: {t}")

    def item_exists_in_kdb(self, item_name: str) -> bool:
        """
        Check if object exists in KDB.

        Args:
            item_name: name in KDB

        Returns:
            bool
        """
        namespace = self._namespace or ""
        query = f"`{item_name} in key`.{namespace}"
        return self.execute_query(query, read_only=True)

    def delete_from_kdb(self, item_name: Optional[str]) -> None:
        """
        Delete object from KDB.

        Args:
            item_name: object name - if None then will delete all objects from namespace
        """
        namespace = self._namespace or ""
        item = item_name or ""
        query = f"delete {item} from `.{namespace}"
        return self.execute_query(query, read_only=False)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        return self.item_exists_in_kdb(table_name)

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
            f"{k}:{self._convert_type_to_kdb_creation(t)}"
            for k, (t, c) in schema.items()
        ]
        column_list = ";".join(columns)

        namespace_prefix = f".{self._namespace}." if self._namespace is not None else ""
        query = f"{namespace_prefix}{table_name}:([]{column_list})"
        self.execute_query(query, read_only=False)

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
        namespace_prefix = f".{self._namespace}." if self._namespace is not None else ""

        query = f"meta `{namespace_prefix}{table_name}"
        keyed_table = self.execute_query(query, read_only=True)
        kdb_types = {
            k[0].decode("ascii"): self._convert_from_kdb_type_to_python(
                v[0].decode("ascii")
            )
            for k, v in keyed_table.iteritems()
        }

        raw_keys = self.execute_query(
            f"keys `{namespace_prefix}{table_name}", read_only=True
        )
        if len(raw_keys) > 0:
            kdb_keys = [k.decode("ascii") for k in raw_keys]
            raise AssertionError(
                f"Primary keys defined in {self._namespace}.{table_name} are not allowed: {kdb_keys}"
            )

        removed = {}
        found = []
        for column, kdb_type in kdb_types.items():
            found.append(column)
            schema_type = schema.get(column)
            if schema_type is None:
                removed[column] = kdb_type
            elif kdb_type != schema_type[0]:
                raise AssertionError(
                    f"Type mismatch found for column {self._namespace}.{table_name}.{column}: kdb={kdb_type} "
                    f"whilst schema={schema_type[0]}"
                )

        added = [k for k in schema.keys() if k not in found]
        if any(added):
            raise AssertionError(
                f"Columns in schema are missing in {self._namespace}.{table_name}: {added}"
            )

        return removed

    @staticmethod
    def _convert_string_filter(column: str, operator: str, value: Any) -> str:
        """
        Convert filter based on str type.

        Args:
            column: column name
            operator: operator
            value: value to compare with

        Returns:
            KDB filter
        """
        if operator in ["in", "not in"]:
            if not isinstance(value, list):
                raise AssertionError(
                    "Filter operator 'in' must be provided a list as value"
                )
            else:
                value_list = ";".join(
                    [KdbDataManager._convert_value(str, v) for v in value]
                )

                if operator == "in":
                    return f"any {column} like/:({value_list})"
                else:
                    return f"not any {column} like/:({value_list})"
        else:
            value = KdbDataManager._convert_value(str, value)
            if operator == "=":
                return f"{column} like {value}"
            elif operator == "!=":
                return f"not {column} like {value}"
            else:
                raise AssertionError(f"Operator not supported for str: {operator}")

    @staticmethod
    def _convert_filter(
        filter: Tuple[str, str, Any], schema: Dict[str, Tuple[type, ColumnCategory]]
    ) -> str:
        """
        Convert filter to KDB query format.

        Args:
            filter: tuple (left operand, operator, right operand)
            schema: table schema

        Returns:
            KDB filter
        """
        column = filter[0]
        if column not in schema.keys():
            raise AssertionError(f"Filter applies to unknown column: {column}")

        operator = filter[1].lower()

        tp = schema[column][0]
        value = filter[2]

        if tp == str:
            return KdbDataManager._convert_string_filter(column, operator, value)
        elif tp == Symbol:
            if operator in ["in", "not in"]:
                if not isinstance(value, list):
                    raise AssertionError(
                        "Filter operator 'in' must be provided a list as value"
                    )
                else:
                    converted_values = [
                        KdbDataManager._convert_value(tp, v) for v in value
                    ]

                    if any(" " in v for v in converted_values):
                        return KdbDataManager._convert_string_filter(
                            column, operator, value
                        )
                    else:
                        value_list = ",".join(converted_values)

                        if operator == "in":
                            return f"{column} in ({value_list})"
                        else:
                            return f"not {column} in ({value_list})"
            else:
                converted_value = KdbDataManager._convert_value(tp, value)

                if " " in converted_value:
                    return KdbDataManager._convert_string_filter(
                        column, operator, value
                    )
                else:
                    if operator == "=":
                        return f"{column}={converted_value}"
                    elif operator == "!=":
                        return f"{column}<>{converted_value}"
                    else:
                        raise AssertionError(
                            f"Operator not supported for Symbol: {operator}"
                        )
        elif tp == bool:
            if operator in ["in", "not in"]:
                if not isinstance(value, list):
                    raise AssertionError(
                        "Filter operator 'in' must be provided a list as value"
                    )
                else:
                    value_list = ",".join(
                        [KdbDataManager._convert_value(tp, v) for v in value]
                    )

                    if operator == "in":
                        return f"{column} in ({value_list})"
                    else:
                        return f"not {column} in ({value_list})"
            else:
                value = KdbDataManager._convert_value(tp, value)
                if operator == "=":
                    return f"{column}={value}"
                elif operator == "!=":
                    return f"{column}<>{value}"
                else:
                    raise AssertionError(f"Operator not supported for bool: {operator}")
        else:
            if operator in ["in", "not in"]:
                if not isinstance(value, list):
                    raise AssertionError(
                        "Filter operator 'in' must be provided a list as value"
                    )
                else:
                    value_list = ",".join(
                        [KdbDataManager._convert_value(tp, v) for v in value]
                    )

                    if operator == "in":
                        return f"{column} in ({value_list})"
                    else:
                        return f"not {column} in ({value_list})"
            else:
                value = KdbDataManager._convert_value(tp, value)

                if operator == "!=":
                    operator = "<>"

                if operator not in ["=", "<>", "<", "<=", ">", ">="]:
                    raise AssertionError(f"Operator not supported for {tp}: {operator}")
                else:
                    return f"{column}{operator}{value}"

    @staticmethod
    def _convert_and_filters(
        filters: List[Tuple[str, str, Any]],
        schema: Dict[str, Tuple[type, ColumnCategory]],
    ) -> str:
        """
        Convert "AND" list of filters to KDB query format.

        Args:
            filters: list of filters
            schema: table schema

        Returns:
            KDB filter
        """
        return "&".join(
            f"({KdbDataManager._convert_filter(f, schema)})" for f in filters
        )

    @staticmethod
    def _convert_or_filters(
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, Tuple[type, ColumnCategory]],
    ) -> str:
        """
        Convert "OR" list of filters to KDB query format.

        Args:
            filters: list of list of filters (super list is OR and sub list is AND)
            schema: table schema

        Returns:
            KDB query filter
        """
        if all(len(f) == 0 for f in filters):
            return ""
        else:
            return "|".join(
                f"({KdbDataManager._convert_and_filters(f, schema)})" for f in filters
            )

    def _build_query_key_list(
        self,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        to_exclude: Optional[List[str]] = None,
    ) -> str:
        """
        Build key list to be used in "fby" filters.

        Args:
            schema: table schema
            to_exclude: list of columns to exclude from the query

        Returns:
            q query part
        """
        to_exclude = to_exclude or []
        keys = [
            name
            for name, (_, cat) in schema.items()
            if cat in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
            and name not in to_exclude
        ]
        return ";".join(keys)

    def _build_base_load_query(
        self,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        columns: Optional[List[str]] = None,
    ) -> str:
        """
        Build base query to load data, without the filters.

        Args:
            table_name: table name
            schema: table schema
            columns: optional list of columns to load, if None then load all

        Returns:
            q query
        """
        namespace_prefix = f".{self._namespace}." if self._namespace is not None else ""

        if columns is not None:
            column_list = ",".join(columns)
        else:
            column_list = ""

        key_list = self._build_query_key_list(schema)

        return (
            f"select {column_list} from {namespace_prefix}{table_name} where "
            f"{column_names.INSERTED_DATETIME}=(max;{column_names.INSERTED_DATETIME}) fby ([]{key_list})"
        )

    def _parse_results(
        self,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        pandas_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Parse result data.

        Args:
            schema: table schema
            pandas_df: pandas data frame

        Returns:
             data frame (actual class type depends on type of calculator)
        """
        types = {n: t for n, (t, _) in schema.items()}

        for col in [
            n for n, t in types.items() if t in [str, Symbol] and n in pandas_df.columns
        ]:
            pandas_df[col] = pandas_df[col].str.decode("ascii")

        # KDB returns dates as numpy.datetime64
        for c in [
            col
            for col, (tp, _) in schema.items()
            if tp == date and col in pandas_df.columns
        ]:
            pandas_df[c] = pandas_df[c].dt.date

        return pandas_df

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
        query = self._build_base_load_query(table_name, schema, columns)

        kdb_filters = ""
        if batch_ids is not None:
            batch_id_list = ",".join([str(b) for b in batch_ids])
            kdb_filters += f", {column_names.BATCH_ID} in ({batch_id_list})"

        if filters is not None:
            kdb_filters += "," + self._convert_or_filters(filters, schema)

        query += kdb_filters
        pandas_df = self.execute_query(query, read_only=True, use_pandas=True)

        pandas_df = self._parse_results(schema, pandas_df)
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
        for n, t in missing_columns.items():
            data = calculator.add_column(data, n, self._get_null_value(t), literal=True)

        namespace_prefix = f".{self._namespace}." if self._namespace is not None else ""
        temp_name = generate_possible_name(lambda n: not self.item_exists_in_kdb(n))

        pandas_df = calculator.to_pandas(data)

        for col, (tp, _) in schema.items():
            if tp in [date, datetime]:
                pandas_df[col] = pd.to_datetime(pandas_df[col])

        query = f"meta `{namespace_prefix}{table_name}"
        keyed_table = self.execute_query(query, read_only=True)
        column_order = [k[0].decode("ascii") for k in keyed_table.iterkeys()]
        pandas_df = pandas_df[column_order]

        try:
            query = f"{{{temp_name}::x}}"
            self.execute_query_with_args(query, pandas_df)

            # Fix table schema - you gotta love KDB
            query = f"meta `{temp_name}"
            keyed_table = self.execute_query(query, read_only=True)
            kdb_types = {
                k[0].decode("ascii"): v[0].decode("ascii")
                for k, v in keyed_table.iteritems()
            }

            if len(kdb_types) != len(schema):
                raise AssertionError(
                    "Internal error: table created in KDB does not contain the expected number of columns"
                )

            for col, (tp, _) in schema.items():
                kdb_type = kdb_types[col]
                expected_type = self._convert_from_python_type_to_kdb(tp)
                if kdb_type != expected_type:
                    if tp == str:
                        query = (
                            f"{temp_name}: update {col}:string({col}) from {temp_name}"
                        )
                    else:
                        query = f'{temp_name}: update "{expected_type}"${col} from {temp_name}'
                    self.execute_query(query, read_only=False)

            query = f"{namespace_prefix}{table_name}:raze ({namespace_prefix}{table_name};{temp_name})"
            self.execute_query(query, read_only=False)
        finally:
            try:
                self.delete_from_kdb(temp_name)
            except Exception:
                logging.exception(f"Could not delete object {temp_name} from KDB")

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
        Load data frame from file.

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
        query = self._build_base_load_query(table_name, schema, columns)

        # Set date filter
        start_date = self._convert_value(date, loading_date - timedelta(lookback))
        end_date = self._convert_value(date, loading_date)
        kdb_filters = f", {column_names.DATA_DATE}>={start_date},{column_names.DATA_DATE}<={end_date}"

        if not group_lookback and batch_ids is not None:
            batch_id_list = ",".join([str(b) for b in batch_ids])
            kdb_filters += f", {column_names.BATCH_ID} in ({batch_id_list})"

        if filters is not None:
            f = self._convert_or_filters(filters, schema)
            if f != "":
                kdb_filters += "," + f

        query += kdb_filters

        if group_lookback or not any(
            name
            for name, (_, cat) in schema.items()
            if cat in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
            and name != column_names.DATA_DATE
        ):
            query = f"select from ({query}) where {column_names.DATA_DATE}=max {column_names.DATA_DATE}"
        else:
            key_list = self._build_query_key_list(schema, [column_names.DATA_DATE])
            query = f"""select from ({query})
 where {column_names.DATA_DATE}=(max;{column_names.DATA_DATE}) fby ([]{key_list})"""

        pandas_df = self.execute_query(query, read_only=True, use_pandas=True)
        pandas_df = self._parse_results(schema, pandas_df)
        pandas_df[column_names.DATA_DATE] = loading_date
        return calculator.from_pandas(pandas_df)

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
        pandas_df = self.execute_query(query, read_only=True, use_pandas=True)
        pandas_df = self._parse_results(
            {k: (v, ColumnCategory.VALUE) for k, v in schema.items()}, pandas_df
        )
        return calculator.from_pandas(pandas_df)
