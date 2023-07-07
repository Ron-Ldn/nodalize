"""Base data node class."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.custom_dependencies.date import DateDependency
from nodalize.data_management.data_manager import DataManager
from nodalize.dependency import DependencyDefinition
from nodalize.dependency_loader import DependencyLoader
from nodalize.orchestration.calculator_factory import CalculatorFactory
from nodalize.orchestration.data_manager_factory import DataManagerFactory
from nodalize.tools.dates import unix_now


class DataNode:
    """Base data node class."""

    def __init__(
        self,
        calculator_factory: CalculatorFactory,
        data_manager_factory: DataManagerFactory,
        node_cache: Any = None,
        **kwargs,
    ) -> None:
        """
        Initialize.

        Args:
            data_manager: instance of DataManager
            calculator_factory: instance of CalculatorFactory
            node_cache: instance of DataNodeCache
        """
        self._calculator_factory = calculator_factory
        self._data_manager_factory = data_manager_factory
        self._calculator = None  # type: Optional[Calculator]
        self._data_manager = None  # type: Optional[DataManager]
        self._data_node_cache = node_cache
        self._dependency_definitions = (
            None
        )  # type: Optional[Dict[str, DependencyDefinition]]
        self._enriched_schema = (
            None
        )  # type: Optional[Dict[str, Tuple[type, ColumnCategory]]]

    def set_dependencies(self, dependencies: Dict[str, DependencyDefinition]) -> None:
        """
        Assign final dependencies.

        Args:
            dependencies: dictionary of dependency definitions
        """
        self._dependency_definitions = dependencies

    @property
    def calculator_type(self) -> Optional[str]:
        """
        Return name of calculator to use.

        Returns:
            calculator name
        """
        return None

    @property
    def calculator(self) -> Calculator:
        """
        Get calculator.

        Returns:
            instance of calculator
        """
        if self._calculator is None:
            self._calculator = self._calculator_factory.get_calculator(
                self.calculator_type
            )
        return self._calculator

    @property
    def data_manager_type(self) -> Optional[str]:
        """
        Return name of data manager to use. If None then use default.

        Returns:
            name of data manager
        """
        return None

    @property
    def data_manager(self) -> DataManager:
        """
        Get data manager.

        Returns:
            instance of data manager
        """
        if self._data_manager is None:
            self._data_manager = self._data_manager_factory.get_data_manager(
                self.data_manager_type
            )
        return self._data_manager

    def get_data_node(self, identifier: str) -> DataNode:
        """
        Get data node definition.

        Args:
            identifier: identifier of the data node to find

        Returns:
            data node definition object
        """
        return self._data_node_cache.get_data_node(identifier)

    def get_dependencies(self) -> Dict[str, DependencyDefinition]:
        """
        Return dependency definitions.

        Returns:
            dictionary of DependencyDefinition
        """
        if self._dependency_definitions is None:
            raise AssertionError(
                "Node was not set up - final dependency definitions are missing"
            )
        else:
            return self._dependency_definitions

    @property
    def identifier(self) -> str:
        """
        Get identifier.

        Returns:
            unique identifier
        """
        return self.__class__.__name__

    @property
    def lookback(self) -> int:
        """
        Get default looback to apply when loading the data.

        Returns:
            default lookback
        """
        return 0

    @property
    def group_lookback(self) -> bool:
        """
        Flag to indicate if the data must be loaded for the same date all at once.

        Returns:
            flag to indicate how to look back
        """
        return False

    @property
    def retry_wait(self) -> int:
        """
        Get time to wait, in seconds, between each tries to generate the data.

        Returns:
            time on seconds
        """
        return 0

    @property
    def retry_timeout(self) -> int:
        """
        Get time window, in second, where the data generation can be retried.

        0 means no retry

        Returns:
            time on seconds
        """
        return 0

    @property
    def schema(self) -> Dict[str, Tuple[type, ColumnCategory]]:
        """
        Get base schema - only columns specific to the node.

        Returns:
            dictionary of columm name/(type, ColumnType)
        """
        return {}

    def get_enriched_schema(self) -> Dict[str, Tuple[type, ColumnCategory]]:
        """
        Get final schema, including generic columns.

        By default, the parameter columns will be DATA_DATE. This can be overriden in derived classes,
            which would require to override "build_dependency".

        Returns:
            dictionary of columm name/(type, ColumnType)
        """
        if self._enriched_schema is None:
            enriched_schema = self.schema.copy()
            enriched_schema[column_names.DATA_DATE] = (date, ColumnCategory.PARAMETER)
            enriched_schema[column_names.INSERTED_DATETIME] = (
                int,
                ColumnCategory.GENERIC,
            )
            enriched_schema[column_names.BATCH_ID] = (int, ColumnCategory.GENERIC)

            self._enriched_schema = enriched_schema
        return self._enriched_schema

    def get_column_names(
        self, includeCategories: Optional[List[ColumnCategory]] = None
    ) -> List[str]:
        """
        Get list of columns from the schema.

        Args:
            includeCategories: optional list of categories to include, if null then include all

        Returns:
            list of column headers
        """
        return [
            c
            for c, t in self.get_enriched_schema().items()
            if includeCategories is None or t[1] in includeCategories
        ]

    @property
    def partitioning(self) -> Optional[List[str]]:
        """
        Get optional list of columns to use for partitioning the table.

        Returns:
            None or list of columns
        """
        return None

    def build_dependency(self) -> DependencyDefinition:
        """
        Build default dependency object.

        Returns:
            DependencyDefinition
        """
        dep = DateDependency(self.identifier)
        dep.assign_data_node(self)
        return dep

    @property
    def dependencies(self) -> Dict[str, Optional[Union[str, DependencyDefinition]]]:
        """
        Get raw dependency definitions.

        Returns:
            dictionary of raw dependency definitions, key can be any string
        """
        return {}

    def load(
        self,
        data_manager: Optional[DataManager] = None,
        calculator: Optional[Calculator] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data.

        Args:
            data_manager: data manager (defaulted to node's one)
            calculator: calculator (defaulted to node's one)
            columns: list of columns to load, all by default
            filters: filters to apply
            batch_ids: optional list of batch id to filter on

        Returns:
            data frame
        """
        data_manager = data_manager or self.data_manager
        calculator = calculator or self.calculator
        return data_manager.load_data_frame(
            calculator,
            self.identifier,
            self.get_enriched_schema(),
            columns,
            filters,
            batch_ids,
        )

    def custom_load(
        self,
        query: str,
        schema: Dict[str, type],
        data_manager: Optional[DataManager] = None,
        calculator: Optional[Calculator] = None,
    ) -> Any:
        """
        Load data based on custom query.

        Args:
            query: custom query - dependent on type of data manager
            schema: types of the columns returned in output
            data_manager: data manager (defaulted to node's one)
            calculator: calculator (defaulted to node's one)

        Returns:
            data frame
        """
        data_manager = data_manager or self.data_manager
        calculator = calculator or self.calculator
        return data_manager.load_data_frame_from_query(calculator, query, schema)

    def compute(
        self,
        parameters: Dict[str, Any],
        **dependency_loaders: DependencyLoader,
    ) -> Any:
        """
        Compute data.

        Args:
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
            inputs: input data

        Returns:
            computed data
        """
        if len(dependency_loaders) == 1:
            loader = next(iter(dependency_loaders.values()))
            return loader()

    def load_and_compute(
        self,
        parameters: Dict[str, Any],
        batch_id: int,
        parent_batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load inputs, compute data and return results.

        Args:
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
            batch_id: batch id to save down
            parent_batch_ids: batch ids of the parent data to load

        Returns:
            the data frame computed
        """
        dependency_loaders = {
            name: DependencyLoader(
                dep, self.data_manager, self.calculator, parameters, parent_batch_ids
            )
            for name, dep in self.get_dependencies().items()
        }
        data = self.compute(parameters, **dependency_loaders)

        if data is None:
            return None

        for parameter_column in self.get_column_names([ColumnCategory.PARAMETER]):
            if parameter_column in parameters:
                parameter_value = parameters[parameter_column]
                data = self.calculator.add_column(
                    data,
                    parameter_column,
                    parameter_value,
                    override=False,
                    literal=True,
                )

        # Generic columns
        data = self.calculator.add_column(
            data,
            column_names.INSERTED_DATETIME,
            unix_now(),
            override=False,
            literal=True,
        )
        data = self.calculator.add_column(
            data, column_names.BATCH_ID, batch_id, override=False, literal=True
        )

        # Remove columns coming from dependencies
        data = self.calculator.select_columns(data, self.get_enriched_schema().keys())

        return data

    def generate_run_parameters(
        self,
        parent_batch_ids: Optional[List[int]],
        parameters: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Generate sets of parameters to run based on delta updates of parent nodes.

        Args:
            parent_batch_ids: optional list of batch ids to narrow down data to load
            parameters: parameters used for parent runs

        Returns:
            list of dictionary, where each dictionary is a set of parameters for the downsteam run
        """
        if parent_batch_ids is None or len(self.dependencies) == 0:
            return [parameters]

        raw_param_list = []
        for dep in self.get_dependencies().values():
            raw_param_list.extend(
                dep.generate_parameters(
                    parameters,
                    parent_batch_ids,
                    self.data_manager,
                    self.calculator,
                )
            )

        if len(raw_param_list) < 2:
            return raw_param_list
        else:
            parameters_set = set(frozenset(d.items()) for d in raw_param_list)
            return [{k: v for k, v in fs} for fs in parameters_set]

    def on_compute_error(self) -> None:
        """Perform custom action when the computation of the node failed."""
        pass

    def on_failure(self) -> None:
        """Perform custom action when data generation failed."""
        pass
