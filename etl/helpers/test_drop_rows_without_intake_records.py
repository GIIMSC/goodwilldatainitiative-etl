import pandas as pd

from etl.helpers import drop_rows_without_intake_records

def test_find_case_numbers():
    response_text = "No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>"
    drop_rows_without_intake_records.find_case_numbers(response_text)


def test_drop_rows_without_intake_records():
    response_text = "No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>"
    dataset = pd.DataFrame(
        data={
            "Date": ["2020-07-28", "2020-03-10", "2020-03-10"],
            "CaseNumber": ["CASEID-abcde", "CASEID-fghijk", "CASEID-xyzxyz"],
            "MilestoneFlag": ["SixtyDays", "SixtyDays", "Intake"],
            "MemberOrganization": ["abc", "abc", "abc"],
        }
    )

    drop_rows_without_intake_records.drop_rows_without_intake_records(dataset, response_text)


