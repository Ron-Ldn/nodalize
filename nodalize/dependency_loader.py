"""Convenient class to encapsulate the loading of a dependency."""

import threading
from typing import Any, Callable, Dict, List, Optional

from nodalize.calculators.calculator import Calculator
from nodalize.data_management.data_manager import DataManager
from nodalize.dependency import DependencyDefinition


class DependencyLoader:
    """Convenient class to encapsulate the loading of a dependency."""

    class AsyncLoader:
        """Asynchronous loader."""

        def __init__(self, func: Callable[[], Any]) -> None:
            """
            Initialize.

            Args:
                func: actual loading function
            """
            self._ret = None

            def exec():
                self._ret = func()

            self._thread = threading.Thread(target=exec)
            self._thread.start()

        def __call__(self):
            """
            Wait for thread to complete and return data.

            Returns:
                data frame
            """
            self._thread.join()
            return self._ret

    def __init__(
        self,
        dependency: DependencyDefinition,
        data_manager: DataManager,
        calculator: Calculator,
        parameters: Dict[str, Any],
        parent_batch_ids: Optional[List[int]],
    ) -> None:
        """
        Initialize.

        Args:
            dependency: dependency definition
            data_manager: data manager
            calculator: calculator
            parameters: optional dictionary of parameters
            parent_batch_ids: optional list of parent batch ids for delta cascading
        """
        self._dependency = dependency
        self._data_manager = data_manager
        self._calculator = calculator
        self._parameters = parameters
        self._parent_batch_ids = parent_batch_ids

    def __call__(
        self,
        parameters_override: Optional[Dict[str, Any]] = None,
        ignore_deltas: bool = False,
        asynchronous: bool = False,
    ) -> Any:
        """
        Load the dependency data.

        Args:
            parameters_override: to override the parameters passed to the initializer
            ignore_deltas: True to ignore the delta cascading

        Returns:
            Any: _description_
        """

        def load():
            return self._dependency.load_data(
                self._data_manager,
                self._calculator,
                parameters_override or self._parameters,
                None if ignore_deltas else self._parent_batch_ids,
            )

        if asynchronous:
            return DependencyLoader.AsyncLoader(load)
        else:
            return load()
