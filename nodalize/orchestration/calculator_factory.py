"""Calculator factory."""
from typing import Any, Dict, Optional

from nodalize.calculators.calculator import Calculator


class CalculatorFactory:
    """Calculator factory."""

    def __init__(self, application_name: str) -> None:
        """
        Initialize factory.

        Args:
            application_name: name of the application
        """
        self._application_name = application_name
        self._calculators = {}  # type: Dict[str, Calculator]

    def set_calculator(
        self, identifier: str, calculator: Optional[Calculator] = None, **kwargs: Any
    ) -> Calculator:
        """
        Define calculator, which can then be accessed by name.

        Args:
            identifier: unique identifier
            calculator: instance of calculator - if None, then will use default calculator
            kwargs: optional parameters to pass to calculator initializer

        Returns:
            calculator added to cache
        """
        if calculator is None:
            if identifier == "pandas":
                from nodalize.calculators.pandas_calculator import PandasCalculator

                calculator = PandasCalculator(self._application_name, **kwargs)
            elif identifier == "pyarrow":
                from nodalize.calculators.pyarrow_calculator import PyarrowCalculator

                calculator = PyarrowCalculator(self._application_name, **kwargs)
            elif identifier == "dask":
                from nodalize.calculators.dask_calculator import DaskCalculator

                calculator = DaskCalculator(self._application_name, **kwargs)
            elif identifier == "spark":
                from nodalize.calculators.spark_calculator import SparkCalculator

                calculator = SparkCalculator(self._application_name, **kwargs)
            elif identifier == "polars":
                from nodalize.calculators.polars_calculator import PolarsCalculator

                calculator = PolarsCalculator(self._application_name, **kwargs)
            else:
                raise ValueError(
                    f"{identifier} not defined, available identifiers are: pandas, pyarrow, pandas, dask"
                )

        self._calculators[identifier] = calculator
        return calculator

    def get_calculator(self, calculator_type: str) -> Calculator:
        """
        Get existing calculator based on identifier.

        Args:
            calculator_type: type of calculator needed

        Returns:
            instance of calculator
        """
        calculator = self._calculators.get(calculator_type)

        if calculator is None:
            try:
                calculator = self.set_calculator(calculator_type)
            except Exception:
                pass

        if calculator is None:
            raise ValueError(f"No calculator defined for {calculator_type}.")

        return calculator
