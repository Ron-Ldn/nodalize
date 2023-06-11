from datetime import date, datetime
from unittest import TestCase

import pandas as pd

from nodalize.tools.dates import (
    datetime_series_to_unix,
    to_unix,
    unix_series_to_datetime,
)


class TestDates(TestCase):
    def test_to_unix(self):
        dt = date(2023, 4, 13)
        unix_time = to_unix(dt)
        self.assertEqual(1681344000000000, unix_time)

    def test_datetime_series_to_unix(self):
        df = pd.DataFrame({"DT": [date(2023, 4, 13), date(2023, 4, 14)]})

        s = datetime_series_to_unix(df["DT"])
        self.assertEqual([1681344000000000, 1681430400000000], s.tolist())

    def test_unix_series_to_datetime(self):
        df = pd.DataFrame({"UDT": [1681344000000000, 1681430400000000]})

        s = unix_series_to_datetime(df["UDT"])
        self.assertEqual([datetime(2023, 4, 13), datetime(2023, 4, 14)], s.tolist())
