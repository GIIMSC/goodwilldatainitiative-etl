import re


def find_case_numbers(response_text: str):
    """
    This function extracts a list case numbers from a string.

    Arguments:
        - the response text from the Gateway API, which appears in the following format:
        ```
        No 'Intake' records found<br/><br/>Case: CASEID-abcde  Member: xxxx-xxxx-xxxxx<br/>Case: CASEID-fghijk  Member: xxxx-xxxx-xxxxx<br/>
        ```
    """
    pattern = r'Case:\s(.*?)\s+Member:'

    return re.findall(pattern, response_text)


def drop_rows_without_intake_records(dataframe, response_text):
    case_numbers = find_case_numbers(response_text)
    dataframe = dataframe[dataframe.CaseNumber.isin(case_numbers) == False]
    
    return dataframe