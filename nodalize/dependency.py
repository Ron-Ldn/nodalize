"""Class storing definition for a data node dependency."""
from typing import Any, Dict, List, Optional, Tuple, Union

from nodalize.calculators.calculator import Calculator
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.data_manager import DataManager


class ParameterValue:
    """Class to store a parameter name, to use in a filter."""

    __slots__ = "_parameter_name"

    def __init__(self, parameter_name: str) -> None:
        """
        Initialize.

        Args:
            parameter_name: name of the parameter to use
        """
        self._parameter_name = parameter_name

    @property
    def parameter_name(self) -> str:
        """
        Get name of the parameter.

        Returns:
            name of the parameter
        """
        return self._parameter_name


class DependencyDefinition:
    """Class storing definition for a data node dependency."""

    __slots__ = (
        "_node_identifier",
        "_filters",
        "_fields",
        "_node",
    )

    def __init__(
        self,
        node_identifier: str,
        *,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        data_fields: Optional[Union[Dict[str, str], List[str]]] = None,
    ) -> None:
        """
        Initialize.

        Args:
            node_identifier: data node identifier
            filters: optional list of filters to apply when loading the data for the node
            data_fields: dictionary where the keys are the fields to load from the data node and the values are the name
                to assign in the final data frame - can also be a simple list of columns to load
        """
        self._node_identifier = node_identifier
        self._node = None  # type: Any
        self._filters = filters or []

        if isinstance(data_fields, list):
            self._fields = {c: c for c in data_fields}  # type: Optional[Dict[str, str]]
        else:
            self._fields = data_fields

    @property
    def data_node_identifier(self) -> str:
        """
        Return data node identifier.

        Returns:
            identifier
        """
        return self._node_identifier

    def assign_data_node(self, node: Any) -> None:
        """
        Assign data node.

        Args:
            node: data node
        """
        self._node = node

    @property
    def data_node(self) -> Any:
        """
        Return data node.

        Returns:
            node
        """
        if self._node is None:
            raise AssertionError("Node was not assigned to dependency")
        return self._node

    @property
    def filters(self) -> List[List[Tuple[str, str, Any]]]:
        """
        Get filters.

        Returns:
            list of filters
        """
        return self._filters

    @property
    def fields(self) -> Dict[str, str]:
        """
        Get fields to load.

        Returns:
            dictionary field name: new column name
        """
        if self._fields is None:
            self._fields = {
                name: name
                for name in self.data_node.get_column_names(
                    [ColumnCategory.KEY, ColumnCategory.VALUE, ColumnCategory.PARAMETER]
                )
            }

        return self._fields

    def populate_fields_to_load(self) -> List[str]:
        """
        Get list of columns to load from the dependency.

        Returns:
            list of column names
        """
        return list(set(self.fields.keys()))

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
        if self.filters is not None and len(self.filters) > 0:
            filters = [
                [
                    (
                        filterDesc[0],
                        filterDesc[1],
                        parameters[filterDesc[2].parameter_name]
                        if isinstance(filterDesc[2], ParameterValue)
                        else filterDesc[2],
                    )
                    for filterDesc in andFilter
                ]
                for andFilter in self.filters
            ]
        else:
            parameter_columns = set(
                self.data_node.get_column_names([ColumnCategory.PARAMETER])
            )
            filters = [
                [(k, "=", v) for k, v in parameters.items() if k in parameter_columns]
            ]

        parent_df = self.data_node.load(
            None,
            calculator,
            self.populate_fields_to_load(),
            filters,
            parent_batch_ids,
        )

        if parent_df is None:
            raise ValueError(f"No data found for data node {self.data_node.identifier}")

        parent_df = calculator.rename_columns(parent_df, self.fields)
        return parent_df

    def generate_parameters(
        self,
        parameters: Dict[str, Any],
        parent_batch_ids: List[int],
        data_manager: DataManager,
        calculator: Calculator,
    ) -> List[Dict[str, Any]]:
        """
        Generate sets of parameters to run based on delta updates of dependency.

        The default implementation is very naive, but will fit a large majority of cases and will be the
            most efficient in term of performance.

        Args:
            parameters: parameters used for parent runs
            parent_batch_ids: list of batch ids to narrow down data to load
            data_manager: data manager
            calculator: instance of calculator

        Returns:
            list of dictionary, where each dictionary is a set of parameters for the downsteam run
        """
        if len(parent_batch_ids) == 0:
            return []
        else:
            return [parameters]
