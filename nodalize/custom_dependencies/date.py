"""Dependency to a DataDate node."""
from typing import Any, Dict, List, Optional, Tuple, Union

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.data_manager import DataManager
from nodalize.dependency import DependencyDefinition


class DateDependency(DependencyDefinition):
    """Dependency to a DataDate node."""

    def __init__(
        self,
        node_identifier: str,
        *,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        data_fields: Optional[Union[Dict[str, str], List[str]]] = None,
        lookback: Optional[int] = None,
        join_on_data_date: bool = True,
    ) -> None:
        """
        Initialize.

        Args:
            node_identifier: data node identifier
            day_lag: lag (in days) to apply when loading the data
            filters: optional list of filters to apply when loading the data for the node
            data_fields: dictionary where the keys are the fields to load from the data node and the values are the name
                to assign in the final data frame - can also be a simple list of columns to load
            lookback: lookback to apply when loading the data (default to node lookback)
            join_on_data_date: if True, then will add a join on DataDate=DataDate, if DataDate is not already used in
                the joins
        """
        DependencyDefinition.__init__(
            self,
            node_identifier,
            filters=filters,
            data_fields=data_fields,
        )
        self._lookback = lookback
        self._join_on_data_date = join_on_data_date

    @property
    def lookback(self) -> int:
        """
        Get lookback value.

        Returns:
            lookback
        """
        return self._lookback or self.data_node.lookback

    def load_data(
        self,
        data_manager: DataManager,
        calculator: Calculator,
        parameters: Dict[str, Any],
        parent_batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load dependency data.

        Args:
            data_manager: data manager
            calculator: calculator
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
            parent_batch_ids: batch ids of the parent data to load

        Returns:
            data frame
        """
        if self.lookback == 0:
            return DependencyDefinition.load_data(
                self, data_manager, calculator, parameters, parent_batch_ids
            )
        else:
            if self.filters is None or len(self.filters) == 0:
                filters = [[]]  # type: List[List[Tuple[str, str, Any]]]
            else:
                filters = self.filters.copy()

            if len(filters) == 1:
                parameter_columns = set(
                    self.data_node.get_column_names([ColumnCategory.PARAMETER])
                )
                filters[0].extend(
                    [
                        (k, "=", v)
                        for k, v in parameters.items()
                        if k in parameter_columns and k != column_names.DATA_DATE
                    ]
                )

            parent_df = data_manager.load_data_frame_with_lookback(
                calculator,
                self.data_node.identifier,
                self.data_node.get_enriched_schema(),
                parameters[column_names.DATA_DATE],
                self.lookback,
                group_lookback=self.data_node.group_lookback,
                columns=self.populate_fields_to_load(),
                filters=filters,
                batch_ids=parent_batch_ids,
            )

            if parent_df is None:
                raise ValueError(
                    f"No data found for data node {self.data_node.identifier}"
                )

            parent_df = calculator.rename_columns(parent_df, self.fields)
            return parent_df
