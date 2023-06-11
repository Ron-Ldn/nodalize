from unittest import TestCase

from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition
from nodalize.orchestration.data_node_cache import DataNodeCache


class DataNode1(DataNode):
    pass


class DataNode2(DataNode):
    pass


class DataNode3(DataNode):
    pass


class DataNode4(DataNode):
    dependencies = {
        "1": DependencyDefinition("DataNode1"),
        "2": DependencyDefinition("DataNode2"),
    }


class DataNode5(DataNode):
    dependencies = {
        "2": DependencyDefinition("DataNode2"),
        "4": DependencyDefinition("DataNode4"),
    }


class DataNode6(DataNode):
    dependencies = {"3": DependencyDefinition("DataNode3")}


class DataNode7(DataNode):
    dependencies = {"3": DependencyDefinition("DataNode3")}


class DataNode8(DataNode):
    dependencies = {"6": DependencyDefinition("DataNode6")}


class DataNode10(DataNode):
    dependencies = {"9": DependencyDefinition("DataNode9")}


def build_cache():
    factory = DataNodeCache()
    factory.add_node(DataNode1(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode2(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode3(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode4(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode5(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode6(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode7(data_manager_factory=None, calculator_factory=None))
    factory.add_node(DataNode8(data_manager_factory=None, calculator_factory=None))
    factory.build_dependency_graph()
    return factory


def build_cache_with_error():
    factory = DataNodeCache()
    factory.add_node(DataNode10(data_manager_factory=None, calculator_factory=None))
    factory.build_dependency_graph()
    return factory


class TestDataNodeCache(TestCase):
    def test_tree_one_node(self):
        factory = build_cache()

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode1"]).levels
        ]
        self.assertEquals([["DataNode1"], ["DataNode4"], ["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode2"]).levels
        ]
        self.assertEquals([["DataNode2"], ["DataNode4"], ["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode3"]).levels
        ]
        self.assertEquals(
            [["DataNode3"], ["DataNode6", "DataNode7"], ["DataNode8"]], levels
        )

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode4"]).levels
        ]
        self.assertEquals([["DataNode4"], ["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode5"]).levels
        ]
        self.assertEquals([["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode6"]).levels
        ]
        self.assertEquals([["DataNode6"], ["DataNode8"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode7"]).levels
        ]
        self.assertEquals([["DataNode7"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(["DataNode8"]).levels
        ]
        self.assertEquals([["DataNode8"]], levels)

    def test_same_tree(self):
        factory = build_cache()

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(
                ["DataNode1", "DataNode2"]
            ).levels
        ]
        self.assertEquals(
            [["DataNode1", "DataNode2"], ["DataNode4"], ["DataNode5"]], levels
        )

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(
                ["DataNode1", "DataNode5"]
            ).levels
        ]
        self.assertEquals([["DataNode1"], ["DataNode4"], ["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(
                ["DataNode1", "DataNode5", "DataNode4"]
            ).levels
        ]
        self.assertEquals([["DataNode1"], ["DataNode4"], ["DataNode5"]], levels)

        levels = [
            [m.identifier for m in level]
            for level in factory.build_downstream_tree(
                ["DataNode3", "DataNode8", "DataNode7"]
            ).levels
        ]
        self.assertEquals(
            [["DataNode3"], ["DataNode6", "DataNode7"], ["DataNode8"]], levels
        )

    def test_bad_reference(self):
        self.assertRaises(KeyError, lambda: build_cache_with_error())
