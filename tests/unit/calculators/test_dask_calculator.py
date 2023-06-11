from unittest import TestCase

from nodalize.calculators.dask_calculator import DaskCalculator
from tests.unit.calculators.base_test_calculator import BaseTestCalculator


class TestDaskCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return DaskCalculator
