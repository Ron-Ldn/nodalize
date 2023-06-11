import time
from unittest import TestCase

from nodalize.dependency_loader import DependencyLoader


class TestDependencyLoader(TestCase):
    class MockDependency:
        def load_data(self, data_manager, calculator, parameters, parent_ids):
            self.async_call = False
            if parameters is not None and parameters["async"]:
                self.async_call = True
                print("Sleeping for 5 seconds")
                time.sleep(5)
                print("Awake!")
            return 42

    def test___call__sync(self):
        dependency = self.MockDependency()
        loader = DependencyLoader(dependency, None, None, None, None)
        self.assertEqual(42, loader())
        self.assertFalse(dependency.async_call)

    def test___call__async(self):
        dependency = self.MockDependency()
        loader = DependencyLoader(dependency, None, None, None, None)
        async_loader = loader(asynchronous=True, parameters_override={"async": True})
        self.assertIsInstance(async_loader, DependencyLoader.AsyncLoader)
        print("Waiting for result")
        self.assertEqual(42, async_loader())
        self.assertTrue(dependency.async_call)
