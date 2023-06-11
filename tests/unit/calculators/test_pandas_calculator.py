from unittest import TestCase

from nodalize.calculators.pandas_calculator import PandasCalculator
from tests.unit.calculators.base_test_calculator import BaseTestCalculator


class TestPandasCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return PandasCalculator
