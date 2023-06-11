from unittest import TestCase

from nodalize.calculators.polars_calculator import PolarsCalculator
from tests.unit.calculators.base_test_calculator import BaseTestCalculator


class TestPolarsCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return PolarsCalculator
