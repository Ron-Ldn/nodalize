from unittest import TestCase

from nodalize.calculators.dask_calculator import DaskCalculator
from nodalize.calculators.pandas_calculator import PandasCalculator
from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from nodalize.calculators.spark_calculator import SparkCalculator
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator


class TestCoordinator(TestCase):
    def test_calculators(self):
        coordinator = Coordinator("somedb")
        self.assertRaises(ValueError, lambda: coordinator.set_calculator("myowncalc"))

        self.assertIsInstance(coordinator.get_calculator("pandas"), PandasCalculator)
        self.assertIsInstance(coordinator.get_calculator("spark"), SparkCalculator)
        self.assertIsInstance(coordinator.get_calculator("dask"), DaskCalculator)
        self.assertIsInstance(coordinator.get_calculator("pyarrow"), PyarrowCalculator)

        coordinator.set_calculator("myowncalc", PandasCalculator("test"))
        self.assertIsInstance(coordinator.get_calculator("myowncalc"), PandasCalculator)

        coordinator.set_calculator("myowncalc", SparkCalculator("test"))
        self.assertIsInstance(coordinator.get_calculator("myowncalc"), SparkCalculator)

        self.assertEqual(5, len(coordinator._calculator_factory._calculators))

    def test_nodes(self):
        class ParentNode(DataNode):
            pass

        class IntermediateNode1(DataNode):
            @property
            def dependencies(self):
                return {"dep": "ParentNode"}

        class IntermediateNode2(DataNode):
            @property
            def dependencies(self):
                return {"dep": "ParentNode"}

        class FinalNode(DataNode):
            @property
            def dependencies(self):
                return {"dep1": "IntermediateNode1", "dep21": "IntermediateNode2"}

        coordinator = Coordinator("somedb")
        parentNode = coordinator.create_data_node(ParentNode)
        intNode1 = coordinator.create_data_node(IntermediateNode1)
        intNode2 = coordinator.create_data_node(IntermediateNode2)
        finalNode = coordinator.create_data_node(FinalNode)
        coordinator.set_up()

        self.assertEqual(
            ["FinalNode", "IntermediateNode1", "IntermediateNode2", "ParentNode"],
            sorted(coordinator.get_data_node_identifiers()),
        )

        allNodes = coordinator.get_data_nodes()
        self.assertEqual(4, len(allNodes))
        self.assertIn(parentNode, allNodes)
        self.assertIn(intNode1, allNodes)
        self.assertIn(intNode2, allNodes)
        self.assertIn(finalNode, allNodes)

        levels = coordinator.get_dependency_graph_levels()
        self.assertEqual(3, len(levels))
        self.assertEqual([parentNode], levels[0])
        self.assertEqual(2, len(levels[1]))
        self.assertIn(intNode1, levels[1])
        self.assertIn(intNode2, levels[1])
        self.assertEqual([finalNode], levels[2])

        levels = coordinator.get_dependency_graph_levels(
            ["IntermediateNode2", "FinalNode"]
        )
        self.assertEqual(2, len(levels))
        self.assertEqual([intNode2], levels[0])
        self.assertEqual([finalNode], levels[1])

    def test_compute_and_save_single_node(self):
        class DummyDataManager:
            def __init__(self, fail):
                self._fail = fail
                self.calls = 0

            def save_updates(self, *args, **kwargs):
                self.calls += 1
                if self._fail:
                    raise ValueError

        data_manager = DummyDataManager(False)

        class CustomDataNode:
            def __init__(self, retry_wait, retry_timeout, **kwargs):
                self._retry_wait = retry_wait
                self._retry_timeout = retry_timeout
                self.compute_error = 0
                self.failure = 0

            @property
            def retry_wait(self):
                return self._retry_wait

            @property
            def retry_timeout(self):
                return self._retry_timeout

            @property
            def identifier(self):
                return ""

            @property
            def partitioning(self):
                return None

            @property
            def calculator(self):
                return PandasCalculator("Test")

            @property
            def data_manager(self):
                return data_manager

            def get_enriched_schema(self):
                return None

            def load_and_compute(self, parameters, batch_id, parent_batch_ids):
                if parameters["mode"] == "success":
                    return "success"
                elif parameters["mode"] == "computeerror":
                    raise AssertionError("Computation error")
                else:
                    raise NotImplementedError

            def on_compute_error(self):
                self.compute_error += 1

            def on_failure(self):
                self.failure += 1

        coordinator = Coordinator("somedb")
        coordinator.set_calculator("pandas")
        node = coordinator.create_data_node(
            CustomDataNode, retry_wait=0, retry_timeout=0
        )

        coordinator.compute_and_save_single_node(node, 1234, {"mode": "success"})

        self.assertEqual(0, node.compute_error)
        self.assertEqual(0, node.failure)
        self.assertEqual(1, data_manager.calls)

        self.assertRaises(
            AssertionError,
            lambda: coordinator.compute_and_save_single_node(
                node, 1234, {"mode": "computeerror"}
            ),
        )

        self.assertEqual(1, node.compute_error)
        self.assertEqual(1, node.failure)
        self.assertEqual(1, data_manager.calls)

        node._retry_wait = 5
        node._retry_timeout = 9

        self.assertRaises(
            AssertionError,
            lambda: coordinator.compute_and_save_single_node(
                node, 1234, {"mode": "computeerror"}
            ),
        )

        self.assertEqual(3, node.compute_error)
        self.assertEqual(2, node.failure)
        self.assertEqual(1, data_manager.calls)

        coordinator.compute_and_save_single_node(node, 1234, {"mode": "success"})

        self.assertEqual(3, node.compute_error)
        self.assertEqual(2, node.failure)
        self.assertEqual(2, data_manager.calls)
