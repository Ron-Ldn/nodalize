"""Base class to manage data files."""

from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.tools.static_func_tools import generate_possible_name


class DataManager(ABC):
    """Base class to manage data files."""

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @staticmethod
    def extract_columns_from_schema(
        schema: Dict[str, Tuple[type, ColumnCategory]], categories: List[ColumnCategory]
    ) -> List[str]:
        """
        Get list of columns from the schema.

        Args:
            schema: table schema
            categories: list of categories to include

        Returns:
            list of column headers
        """
        return [c for c, t in schema.items() if t[1] in categories]

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
        # 1 Build filters
        if filters is None or len(filters) == 0:
            final_filters = [[]]  # type: List[List[Tuple[str, str, Any]]]
        else:
            final_filters = filters.copy()

        if len(final_filters) == 1:
            start_date = loading_date - timedelta(lookback)
            end_date = loading_date
            final_filters[0].append((column_names.DATA_DATE, ">=", start_date))
            final_filters[0].append((column_names.DATA_DATE, "<=", end_date))

        # 2 Load all the data.
        parent_df = self.load_data_frame(
            calculator,
            table_name,
            schema,
            columns,
            final_filters,
        )

        if parent_df is None:
            return None

        key_columns_ex_date = [
            c
            for c in self.extract_columns_from_schema(
                schema, [ColumnCategory.KEY, ColumnCategory.PARAMETER]
            )
            if c != column_names.DATA_DATE
        ]

        # 3 Filter
        if group_lookback or len(key_columns_ex_date) == 0:
            # Since the data must be selected as a block (all for the same date), then it does
            # not make sense to filter on batch id anymore.
            # We will get the most recent date and filter the data on that date.
            max_date = max(
                calculator.extract_unique_column_values(
                    column_names.DATA_DATE, parent_df
                )
            )
            parent_df = calculator.apply_filter(
                parent_df,
                (column_names.DATA_DATE, "=", max_date),
                {col: tp for col, (tp, _) in schema.items()},
            )
        else:
            if batch_ids is not None:
                # We want to filter on keys related to batch ids.
                # We will first determine the list of keys impacted by the batch ids and thus build a data frame.
                # Then we will join the data with that data frame.
                keys_df = self.load_data_frame(
                    calculator,
                    table_name,
                    schema,
                    key_columns_ex_date,
                    final_filters,
                    batch_ids=batch_ids,
                )

                keys_df = calculator.drop_duplicates(keys_df)
                forbidden_column_names = self.extract_columns_from_schema(
                    schema,
                    [
                        ColumnCategory.KEY,
                        ColumnCategory.PARAMETER,
                        ColumnCategory.VALUE,
                    ],
                )
                dummy_col = generate_possible_name(
                    lambda n: n not in forbidden_column_names
                )
                keys_df = calculator.add_column(keys_df, dummy_col, 1, literal=True)

                # Now join, filter and delete dummy column
                parent_df = calculator.left_join_data_frames(
                    parent_df,
                    keys_df,
                    key_columns_ex_date,
                )
                parent_df = calculator.apply_filter(
                    parent_df,
                    (dummy_col, "=", 1),
                    {col: tp for col, (tp, _) in schema.items()},
                )
                parent_df = calculator.drop_columns(parent_df, [dummy_col])

            # Keep latest data
            parent_df = calculator.filter_in_max_values(
                parent_df,
                self.extract_columns_from_schema(schema, [ColumnCategory.VALUE]),
                key_columns_ex_date,
            )

        parent_df = calculator.add_column(
            parent_df, column_names.DATA_DATE, loading_date, literal=True
        )

        return parent_df
