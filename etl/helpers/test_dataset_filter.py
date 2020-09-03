import pytest
import pandas as pd

from etl.helpers import dataset_filter


@pytest.mark.parametrize(
    "response_text,expected_case_numbers",
    [
        (
            "No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>",
            ["CASEID-abcde", "CASEID-fghijk"],
        ),
        # Test that regex handles lots of whitespace
        (
            "No 'Intake' records found<br/><br/>Case:    CASEID-abcde    Member: xxxx-xxxx-xxxxx<br/>Case:   CASEID-fghijk     Member: xxxx-xxxx-xxxxx<br/>",
            ["CASEID-abcde", "CASEID-fghijk"],
        ),
        # Test that function handles an empty string
        ("", []),
    ],
)
def test_find_case_numbers(response_text, expected_case_numbers):
    output = dataset_filter.find_case_numbers(response_text)

    assert output.sort() == expected_case_numbers.sort()


# TODO: New test!
# def test_drop_rows_without_intake_records():
#     response_text = "No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>"
#     input_dataframe = pd.DataFrame(
#         data={
#             "Date": ["2020-07-28", "2020-03-10", "2020-03-10"],
#             "CaseNumber": ["CASEID-abcde", "CASEID-fghijk", "CASEID-xyzxyz"],
#             "MilestoneFlag": ["SixtyDays", "SixtyDays", "Intake"],
#             "MemberOrganization": ["abc", "abc", "abc"],
#         }
#     )

#     expected_dataframe = pd.DataFrame(
#         data={
#             "Date": ["2020-03-10"],
#             "CaseNumber": ["CASEID-xyzxyz"],
#             "MilestoneFlag": ["Intake"],
#             "MemberOrganization": ["abc"],
#         }
#     )

#     output_dataframe = dataset_filter.drop_rows_without_intake_records(
#         input_dataframe, response_text
#     )

#     pd.util.testing.assert_frame_equal(expected_dataframe, output_dataframe)
