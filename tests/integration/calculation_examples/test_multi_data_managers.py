import os
from datetime import date
from unittest import TestCase

import pandas as pd

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator
from tests.common import compare_data_frames, use_temp_folder


class BaseDataSet(DataNode):
    @property
    def schema(self):
        return {
            "Id": (int, ColumnCategory.KEY),
            "Numerator": (float, ColumnCategory.VALUE),
            "Denominator": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    @property
    def data_manager_type(self):
        return "sqlite"

    def compute(self, parameters):
        return pd.DataFrame(
            {
                "Id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "Numerator": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0],
                "Denominator": [10, 5, 30, 0.25, 25, 3, 0.7, 4, 3],
            }
        )


class EnrichedDataSet(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def data_manager_type(self):
        return "parquet"

    @property
    def schema(self):
        """Define storage format."""
        return {
            "Id": (int, ColumnCategory.KEY),
            "Ratio": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        """Define parent nodes."""
        return {"base_data": "BaseDataSet"}

    def compute(self, parameters, base_data):
        """Compute data."""
        df = base_data()
        df = self.calculator.add_column(
            df,
            "Ratio",
            self.calculator.get_column(df, "Numerator")
            / self.calculator.get_column(df, "Denominator"),
        )
        return df


class TestMultiDataManagers(TestCase):
    def setUp(self):
        self.temp_directory = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "temp"
        )

    @use_temp_folder
    def test_multi_data_managers(self):
        # Set-up environment
        coordinator = Coordinator("test")
        coordinator.set_data_manager(
            "sqlite", SqliteDataManager(os.path.join(self.temp_directory, "test.db"))
        )
        coordinator.set_data_manager(
            "parquet", LocalFileDataManager(self.temp_directory)
        )

        coordinator.create_data_node(BaseDataSet)
        enriched_data_set_node = coordinator.create_data_node(EnrichedDataSet)
        coordinator.set_up()

        # Compute
        coordinator.run_recursively(
            node_identifiers=["BaseDataSet"],
            global_parameters={"DataDate": date(2022, 1, 2)},
        )

        # Reload saved data
        pandas_data = coordinator.get_data_manager("parquet").load_data_frame(
            enriched_data_set_node.calculator,
            enriched_data_set_node.identifier,
            enriched_data_set_node.schema,
        )

        # Check data
        expected_data = pd.DataFrame(
            {
                "Id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "Ratio": [1, 4, 1, 160, 2, 20, 100, 20, 30],
                "DataDate": [date(2022, 1, 2)] * 9,
            }
        )

        compare_data_frames(
            expected_data,
            pandas_data.drop(
                columns=[column_names.INSERTED_DATETIME, column_names.BATCH_ID]
            ).sort_values(by=["Id"]),
        )
