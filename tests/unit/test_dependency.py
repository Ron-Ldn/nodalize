from unittest import TestCase

from nodalize.constants.column_category import ColumnCategory
from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition


class TestDependency(TestCase):
    def test_defaults(self):
        class DataNode1(DataNode):
            def get_enriched_schema(self):
                return {
                    "A": (int, ColumnCategory.KEY),
                    "B": (str, ColumnCategory.KEY),
                    "C": (float, ColumnCategory.VALUE),
                }

        dep = DependencyDefinition("DataNode1")
        dep.assign_data_node(
            DataNode1(data_manager_factory=None, calculator_factory=None)
        )

        self.assertEqual("DataNode1", dep.data_node_identifier)
        self.assertEqual([], dep.filters)
        self.assertEqual({"A": "A", "B": "B", "C": "C"}, dep.fields)

    def test_overrides(self):
        dep = DependencyDefinition(
            "DataNode1",
            filters=[[("Col1", "=", 1.23), ("Col2", "=", 2.34)]],
            data_fields={"Col1": "NewCol1", "Col2": "NewCol2"},
        )

        self.assertEqual("DataNode1", dep.data_node_identifier)
        self.assertEqual([[("Col1", "=", 1.23), ("Col2", "=", 2.34)]], dep.filters)
        self.assertEqual({"Col1": "NewCol1", "Col2": "NewCol2"}, dep.fields)

        dep = DependencyDefinition(
            "DataNode1",
            filters=[[("Col1", "=", 1.23), ("Col2", "=", 2.34)]],
            data_fields=["Col1", "Col2"],
        )

        self.assertEqual({"Col1": "Col1", "Col2": "Col2"}, dep.fields)
