from unittest import TestCase

from nodalize.calculators.pyarrow_calculator import PyarrowCalculator
from tests.unit.calculators.base_test_calculator import BaseTestCalculator


class TestPyarrowCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return PyarrowCalculator
