import unittest
import datetime

from etl.helpers import salesforce


class SalesforceTest(unittest.TestCase):
    def test_parse_sf_record(self):
        input_record = {
            "field": "value",
            "field2": {
                "subfield": "value",
                "attributes": {"ignoredfield": "ignoredvalue"},
            },
            "attributes": {"ignoredfield": "ignoredvalue"},
        }

        actual_parsed_record = salesforce.parse_sf_record(input_record)

        expected_parsed_record = {"field": "value", "field2.subfield": "value"}

        self.assertEqual(expected_parsed_record, actual_parsed_record)


if __name__ == "__main__":
    unittest.main()
