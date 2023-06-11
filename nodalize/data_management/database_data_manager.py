"""Base class for data managers leveraging database technologies."""
import logging
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from nodalize.calculators.calculator import Calculator
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.data_manager import DataManager


class DatabaseDataManager(DataManager):
    """Base class for data managers leveraging database technologies."""

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists.

        Args:
            table_name: name of the table

        Returns:
            True if table exists
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

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
        if not self.table_exists(table_name):
            return None
        else:
            return self.load_data_from_database(
                calculator,
                table_name,
                schema,
                columns,
                filters,
                batch_ids,
            )

    @abstractmethod
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
        raise NotImplementedError

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
        if calculator.row_count(dataframe) > 0 or calculator.calc_type == "spark":
            if self.table_exists(table_name):
                logging.info(f"Checking schema of existing table: {table_name}")
                columns_missing_from_schema = self.check_table_schema(
                    table_name, schema
                )

                if (
                    columns_missing_from_schema is not None
                    and len(columns_missing_from_schema) > 0
                ):
                    logging.info(
                        f"Columns found in table, missing from data: {columns_missing_from_schema}"
                    )
            else:
                logging.info(f"Creating table: {table_name}")
                self.create_table(table_name, schema, partitioning)
                columns_missing_from_schema = {}

            self.add_data_to_table(
                calculator, table_name, schema, columns_missing_from_schema, dataframe
            )
        else:
            logging.info(f"No data to save for {table_name}")
