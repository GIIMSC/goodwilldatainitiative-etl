import os
import pandas as pd
import pytest
import pkg_resources

from etl.helpers import dataset_filter

TEST_DIR = pkg_resources.resource_filename("testfiles", "")
MI_TEST_DIR = os.path.join(TEST_DIR, "mission_impact/")
MI_DATAFILE = os.path.join(MI_TEST_DIR, "fake_transformed_data.csv")
RESPONSE_TEXT = "No 'Intake' records found<br/><br/>Case: CASEID-000001  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-000003  Member: xxxx-xxxx-xxxxx<br/>"


@pytest.mark.parametrize(
    "response_text,expected_case_numbers",
    [
        (RESPONSE_TEXT, ["CASEID-000001", "CASEID-000003"]),
        # Test that regex handles lots of whitespace
        (
            "No 'Intake' records found<br/><br/>Case:    CASEID-000001    Member: xxxx-xxxx-xxxxx<br/>Case:   CASEID-000003     Member: xxxx-xxxx-xxxxx<br/>",
            ["CASEID-000001", "CASEID-000003"],
        ),
        # Test that function handles an empty string
        ("", []),
    ],
)
def test_find_case_numbers(response_text, expected_case_numbers):
    output = dataset_filter.find_case_numbers(response_text)

    assert output.sort() == expected_case_numbers.sort()


def test_from_csv_drop_rows_without_intake_records():
    tempfile_name = dataset_filter.from_csv_drop_rows_without_intake_records(
        datafile_name=MI_DATAFILE, response_text=RESPONSE_TEXT
    )
    tf = open(tempfile_name)
    lines = tf.readlines()

    # Assert that first line still has header/column value
    assert "CaseNumber" in lines[0]
    # Assert that second and lines have 'good' case numbers
    assert "CASEID-000002" in lines[1]
    assert "CASEID-000004" in lines[2]

    tf.seek(0)
    full_text = tf.read()
    # Assert that 'bad' case numbers were removed
    assert "CASEID-000001" not in full_text
    assert "CASEID-000003" not in full_text

    # Clean up
    os.remove(tempfile_name)


# TODO: New test!
# def test_drop_rows_without_intake_records():
#     response_text = RESPONSE_TEXT
#     input_dataframe = pd.DataFrame(
#         data={
#             "Date": ["2020-07-28", "2020-03-10", "2020-03-10"],
#             "CaseNumber": ["CASEID-000001", "CASEID-000003", "CASEID-xyzxyz"],
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
