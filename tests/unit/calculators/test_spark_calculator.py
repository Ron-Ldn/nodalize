from unittest import TestCase

from nodalize.calculators.spark_calculator import SparkCalculator
from tests.unit.calculators.base_test_calculator import BaseTestCalculator


class TestSparkCalculator(TestCase, BaseTestCalculator):
    @property
    def calculator_type(self):
        return SparkCalculator
