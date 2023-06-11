from datetime import date
from unittest import TestCase
from unittest.mock import Mock, patch

from nodalize.constants import column_names
from nodalize.custom_dependencies.lag import LagDependency, WeekDayLagDependency


class TestLagDependency(TestCase):
    @patch("nodalize.custom_dependencies.date.DateDependency.load_data")
    def test_load_data(self, load_data):
        column_names.DATA_DATE = "MyDate"
        load_data.return_value = "control value"
        current_date = date(2023, 4, 26)

        calculator = Mock()

        def add_column(df, column, dt, literal):
            self.assertEqual(date(2023, 4, 26), dt)
            self.assertEqual("control value", df)
            self.assertEqual("MyDate", column)
            return "all good"

        calculator.add_column.side_effect = add_column

        dependency = LagDependency("nd", 2)

        self.assertEqual(
            "all good", dependency.load_data(None, calculator, {"MyDate": current_date})
        )
        load_data.assert_called_once_with(
            dependency, None, calculator, {"MyDate": date(2023, 4, 24)}, None
        )

    @patch("nodalize.custom_dependencies.date.DateDependency.load_data")
    def test_load_data_weekend(self, load_data):
        column_names.DATA_DATE = "MyDate"
        load_data.return_value = "control value"
        current_date = date(2023, 4, 26)

        calculator = Mock()

        def add_column(df, column, dt, literal):
            self.assertEqual(date(2023, 4, 26), dt)
            self.assertEqual("control value", df)
            self.assertEqual("MyDate", column)
            return "all good"

        calculator.add_column.side_effect = add_column

        dependency = LagDependency("nd", 3)

        self.assertEqual(
            "all good", dependency.load_data(None, calculator, {"MyDate": current_date})
        )
        load_data.assert_called_once_with(
            dependency, None, calculator, {"MyDate": date(2023, 4, 23)}, None
        )


class TestWeekDayLagDependency(TestCase):
    @patch("nodalize.custom_dependencies.date.DateDependency.load_data")
    def test_load_data(self, load_data):
        column_names.DATA_DATE = "MyDate"
        load_data.return_value = "control value"
        current_date = date(2023, 4, 26)

        calculator = Mock()

        def add_column(df, column, dt, literal):
            self.assertEqual(date(2023, 4, 26), dt)
            self.assertEqual("control value", df)
            self.assertEqual("MyDate", column)
            return "all good"

        calculator.add_column.side_effect = add_column

        dependency = WeekDayLagDependency("nd", 2)

        self.assertEqual(
            "all good", dependency.load_data(None, calculator, {"MyDate": current_date})
        )
        load_data.assert_called_once_with(
            dependency, None, calculator, {"MyDate": date(2023, 4, 24)}, None
        )

    @patch("nodalize.custom_dependencies.date.DateDependency.load_data")
    def test_load_data_weekend(self, load_data):
        column_names.DATA_DATE = "MyDate"
        load_data.return_value = "control value"
        current_date = date(2023, 4, 26)

        calculator = Mock()

        def add_column(df, column, dt, literal):
            self.assertEqual(date(2023, 4, 26), dt)
            self.assertEqual("control value", df)
            self.assertEqual("MyDate", column)
            return "all good"

        calculator.add_column.side_effect = add_column

        dependency = WeekDayLagDependency("nd", 3)

        self.assertEqual(
            "all good", dependency.load_data(None, calculator, {"MyDate": current_date})
        )
        load_data.assert_called_once_with(
            dependency, None, calculator, {"MyDate": date(2023, 4, 21)}, None
        )
