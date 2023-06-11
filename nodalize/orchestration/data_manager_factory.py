"""Calculator factory."""
from typing import Dict, Optional

from nodalize.data_management.data_manager import DataManager


class DataManagerFactory:
    """DataManager factory."""

    def __init__(self) -> None:
        """Initialize."""
        self._data_managers = {}  # type: Dict[str, DataManager]
        self._default_data_manager = None  # type: Optional[DataManager]

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
        self._data_managers[identifier] = data_manager
        if default:
            self._default_data_manager = data_manager

    def get_data_manager(self, data_manager_type: Optional[str] = None) -> DataManager:
        """
        Get existing data manager based on identifier.

        Args:
            data_manager_type: type of data manager needed. If none, then return default one.

        Returns:
            instance of data manager
        """
        if len(self._data_managers) == 0:
            raise AssertionError("No data manager defined")

        if data_manager_type is None:
            if self._default_data_manager is not None:
                return self._default_data_manager
            else:
                raise AssertionError("No default data manager defined")
        else:
            data_manager = self._data_managers.get(data_manager_type)

            if data_manager is None:
                raise ValueError(f"No data manager defined for {data_manager_type}.")

            return data_manager
