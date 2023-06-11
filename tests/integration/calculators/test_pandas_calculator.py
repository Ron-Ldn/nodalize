import os
from unittest import TestCase

from nodalize.calculators.pandas_calculator import PandasCalculator
from tests.common import use_temp_folder
from tests.integration.calculators.base_test_calculator import BaseTestCalculator


class TestIntegrationPandasCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return PandasCalculator

    @property
    def temp_directory(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")

    @use_temp_folder
    def test_interoperability_no_partition(self):
        self.run_test_interoperability(partitioning=False)

    @use_temp_folder
    def test_interoperability_with_partition(self):
        self.run_test_interoperability(partitioning=True)
