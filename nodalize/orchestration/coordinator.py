"""General and global factory."""
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from nodalize.calculators.calculator import Calculator
from nodalize.data_management.data_manager import DataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.calculator_factory import CalculatorFactory
from nodalize.orchestration.data_manager_factory import DataManagerFactory
from nodalize.orchestration.data_node_cache import DataNodeCache
from nodalize.tools.dates import unix_now


class Coordinator:
    """General and global factory."""

    def __init__(
        self,
        application_name: str,
    ) -> None:
        """
        Initialize factory.

        Args:
            application_name: name of the application
        """
        self._calculator_factory = CalculatorFactory(application_name)
        self._data_manager_factory = DataManagerFactory()
        self._cache = DataNodeCache()

    def set_calculator(
        self, identifier: str, calculator: Optional[Calculator] = None, **kwargs: Any
    ) -> Calculator:
        """
        Define calculator, which can then be accessed by name.

        Args:
            identifier: unique identifier
            calculator: instance of calculator - if None, then will use default calculator
            kwargs: optional parameters to pass to calculator initializer

        Returns:
            calculator added to cache
        """
        return self._calculator_factory.set_calculator(identifier, calculator, **kwargs)

    def get_calculator(self, calculator_type: str) -> Calculator:
        """
        Get existing calculator based on identifier.

        Args:
            calculator_type: type of calculator needed

        Returns:
            instance of calculator
        """
        return self._calculator_factory.get_calculator(calculator_type)

    def set_data_manager(
        self, identifier: str, data_manager: DataManager, default: bool = False
    ) -> None:
        """
        Define data manager, which can then be accessed by name.

        Args:
            identifier: unique identifier
            data_manager: instance of DataManager
            default: set data manager as default
        """
        self._data_manager_factory.set_data_manager(identifier, data_manager, default)

    def get_data_manager(self, data_manager_type: Optional[str] = None) -> DataManager:
        """
        Get existing data manager based on identifier.

        Args:
            data_manager_type: type of data manager needed. If none, then return default one.

        Returns:
            instance of data manager
        """
        return self._data_manager_factory.get_data_manager(data_manager_type)

    def create_data_node(self, data_node_type: type, **kwargs: Any) -> DataNode:
        """
        Instantiate data node.

        Args:
            data_node_type: DataNode derived class

        Returns:
            new node
        """
        node = data_node_type(
            calculator_factory=self._calculator_factory,
            data_manager_factory=self._data_manager_factory,
            node_cache=self._cache,
            **kwargs,
        )
        self._cache.add_node(node)
        return node

    def get_data_node(self, identifier: str) -> DataNode:
        """
        Get data node definition.

        Args:
            identifier: identifier of the data node to find

        Returns:
            data node instance
        """
        return self._cache.get_data_node(identifier)

    def get_data_nodes(self) -> List[DataNode]:
        """
        Get all data nodes.

        Returns:
            list of ndoes
        """
        return list(self._cache._nodes.values())

    def get_data_node_identifiers(self) -> List[str]:
        """
        Get all data node identifiers.

        Returns:
            list of identifiers
        """
        return list(self._cache._nodes.keys())

    def get_dependency_graph_levels(
        self, identifiers: Optional[List[str]] = None
    ) -> List[List[DataNode]]:
        """
        Get levels of dependency graph as list of lists.

        Args:
            list of ids to load, if None then load everything

        Returns:
            list of levels, each level is a list of node identifiers
        """
        identifiers = identifiers or self.get_data_node_identifiers()
        return self._cache.build_downstream_tree(identifiers).levels

    def set_up(self):
        """Set up all nodes."""
        self._cache.build_dependency_graph()

    def compute_and_save_single_node(
        self,
        node: DataNode,
        new_batch_id: int,
        parameters: Dict[str, Any],
        parent_batch_ids: Optional[List[int]] = None,
    ) -> None:
        """
        Compute data and save node.

        Args:
            node: data node
            new_batch_id: batch id to save down
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
            parent_batch_ids: batch ids of the parent data to load
        """
        logging.info(f"Starting computation of data node: {node.identifier}")

        retry_wait = node.retry_wait
        retry_timeout = node.retry_timeout

        start = time.time()
        timeout = start + retry_timeout

        if retry_timeout > 0:
            logging.info(f"Started at {start}, time out at {timeout}")

        retry = True
        success = False
        last_error = None

        while retry:
            try:
                data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)

                if data is None:
                    logging.info(f"No data returned {node.identifier}")
                    return
            except Exception as e:
                logging.exception(f"Failed to compute {node.identifier}")
                node.on_compute_error()
                last_error = str(e)
            else:
                logging.info(f"Saving data produced by data node: {node.identifier}")
                node.data_manager.save_updates(
                    node.calculator,
                    node.identifier,
                    node.get_enriched_schema(),
                    node.partitioning,
                    new_batch_id,
                    data,
                )
                retry = False
                success = True
                logging.info(f"Data saved by data node: {node.identifier}")

            if retry:
                next_try = time.time() + retry_wait
                if next_try < timeout:
                    logging.info(f"Next try scheduled to start at {next_try}")
                    logging.info(f"Going to sleep for {retry_wait} seconds")
                    time.sleep(retry_wait)
                    logging.info(f"Will retry {node.identifier} now")
                else:
                    retry = False
                    logging.info("Time out reached for {node.identifier}")

        if not success:
            node.on_failure()
            raise AssertionError(f"Failed to generate {node.identifier}: {last_error}")

    def compute_and_save(
        self, node_identifier: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Compute data and save node.

        Args:
            node_identifier: node identifier
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
        """
        parameters = parameters or {}
        node = self._cache.get_data_node(node_identifier)

        batch_id = self.generate_batch_id()
        logging.info(f"Batch id generated: {batch_id}")

        self.compute_and_save_single_node(node, batch_id, parameters)

    def generate_batch_parameters(
        self,
        batch_id: int,
        node_identifiers: List[str],
        global_parameters: Optional[Dict[str, Any]] = None,
        specific_parameters: Optional[Dict[str, Dict[str, Any]]] = None,
        parent_batch_ids: Optional[List[int]] = None,
    ) -> Tuple[
        List[Tuple[DataNode, int, Dict[str, Any], Optional[List[int]]]], List[str]
    ]:
        """
        Compute and save required data nodes, and cascade to downstream dependencies.

        Args:
            batch_id: batch id
            node_identifiers: list of node identifiers
            global_parameters: simple dictionary for global parameters
            specific_parameters: dictionary where key is the node id and value is a dictionary
            parent_batch_ids: optional list of batch ids to narrow down data to load

        Returns:
            List of arguments to pass to compute_and_save_single_node for each node + list of children nodes
        """
        if len(node_identifiers) == 0:
            raise AssertionError("Node identifiers missing")

        levels = self._cache.build_downstream_tree(node_identifiers).levels

        global_parameters = global_parameters or {}
        specific_parameters = specific_parameters or {}

        run_parameters = []
        for data_node in levels[0]:
            parameters = global_parameters.copy()
            for key, value in (
                specific_parameters.get(data_node.identifier) or {}
            ).items():
                parameters[key] = value

            parameter_set = data_node.generate_run_parameters(
                parent_batch_ids, parameters
            )

            for p in parameter_set:
                run_parameters.append((data_node, batch_id, p, parent_batch_ids))

        if len(levels) > 1:
            next_level = [node.identifier for node in levels[1]]
        else:
            next_level = []

        return run_parameters, next_level

    def run_sequentially(
        self,
        run_parameters: List[Tuple[DataNode, int, Dict[str, Any], Optional[List[int]]]],
    ) -> None:
        """
        Compute and save multiple nodes sequentially.

        Args:
            run_parameters: List of arguments to pass to compute_and_save_single_node for each node
        """
        errors = []  # type: List[str]
        for args in run_parameters:
            try:
                self.compute_and_save_single_node(*args)
            except Exception as e:
                logging.exception(
                    f"Failed to compute and save {args[0].identifier}: {str(e)}"
                )
                errors.append(str(e))

        if any(errors):
            msg = "\n".join(errors)
            raise AssertionError(msg)

    def generate_batch_id(self) -> int:
        """
        Generate batch id.

        Default will be using unix timestamp. It might be needed to have specific formats for batch ids based
            on technology used, the main pain point being the max size of integers supported.

        Returns:
            batch id
        """
        return unix_now()

    def run_recursively(
        self,
        node_identifiers: List[str],
        global_parameters: Optional[Dict[str, Any]] = None,
        specific_parameters: Optional[Dict[str, Dict[str, Any]]] = None,
        parent_batch_ids: Optional[List[int]] = None,
    ) -> None:
        """
        Compute and save required data nodes, and cascade to downstream dependencies.

        Args:
            node_identifiers: list of node identifiers
            global_parameters: simple dictionary for global parameters
            specific_parameters: dictionary where key is the node id and value is a dictionary
            parent_batch_ids: optional list of batch ids to narrow down data to load
        """
        batch_id = self.generate_batch_id()
        logging.info(f"Batch Id generated: {batch_id}")

        run_parameters, next_level = self.generate_batch_parameters(
            batch_id,
            node_identifiers,
            global_parameters,
            specific_parameters,
            parent_batch_ids,
        )
        self.run_sequentially(run_parameters)

        if parent_batch_ids is None:
            parent_batch_ids = [batch_id]
        else:
            parent_batch_ids.append(batch_id)

        while len(next_level) > 0:
            run_parameters, next_level = self.generate_batch_parameters(
                batch_id,
                next_level,
                global_parameters,
                specific_parameters,
                parent_batch_ids,
            )
            self.run_sequentially(run_parameters)
