import unittest
import datetime

from etl.helpers import dates


class DatesTest(unittest.TestCase):
    def test_get_date_range(self):
        start_date = datetime.datetime(2018, 1, 1)
        end_date = datetime.datetime(2018, 10, 1)

        actual_start_datetime, actual_end_datetime = dates.get_date_range(
            start_date, end_date
        )

        expected_start_datetime = datetime.datetime(2018, 1, 1, 0, 0, 0).isoformat()
        expected_end_datetime = datetime.datetime(2018, 10, 1, 23, 59, 59).isoformat()

        self.assertEqual(expected_start_datetime, actual_start_datetime)
        self.assertEqual(expected_end_datetime, actual_end_datetime)


if __name__ == "__main__":
    unittest.main()
