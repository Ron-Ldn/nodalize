from unittest import TestCase

from nodalize.calculators.dask_calculator import DaskCalculator
from nodalize.calculators.pandas_calculator import PandasCalculator
from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from nodalize.calculators.spark_calculator import SparkCalculator
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
