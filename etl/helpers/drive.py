import pandas as pd
from typing import Dict, List

from googleapiclient import discovery
from googleapiclient.discovery import Resource
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

SheetInfo = Dict[str, str]

SheetTitle = str
SheetTitles = List[SheetTitle]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents.readonly",
]

SHEET_RANGE_SPLIT_CHAR = "!"


def get_google_service(account_info: Dict, api: str, api_version: str) -> Resource:
    """Returns a Google API Resource for a given account authorization, api type, and
    version."""
    credentials: Credentials = Credentials.from_service_account_info(
        account_info, scopes=SCOPES
    )
    return discovery.build(
        api,
        api_version,
        credentials=credentials,
        cache_discovery=False,  # Silence caching warning with Google API client
    )


def get_google_sheets_service(account_info: Dict) -> Resource:
    """Returns a Google Sheets API Resource."""
    return get_google_service(account_info, "sheets", "v4")


def get_google_docs_service(account_info: Dict) -> Resource:
    """Returns a Google Docs API Resource."""
    return get_google_service(account_info, "docs", "v1")


def get_sheets_for_spreadsheet(
    service: Resource, spreadsheet_id: str
) -> List[SheetInfo]:
    """Returns the titles of the sheets in a provided Google Sheet."""
    spreadsheet_info: Dict = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields="sheets.properties(sheetId,title)"
    ).execute()

    return [
        {
            "title": sheet["properties"]["title"],
            "sheetId": sheet["properties"]["sheetId"],
        }
        for sheet in spreadsheet_info["sheets"]
    ]


def get_sheet_titles_from_sheets(sheets: List[SheetInfo]) -> SheetTitles:
    return [sheet["title"] for sheet in sheets]


def get_sheet_titles_for_spreadsheet(
    service: Resource, spreadsheet_id: str
) -> SheetTitles:
    sheets: List[SheetInfo] = get_sheets_for_spreadsheet(service, spreadsheet_id)
    return get_sheet_titles_from_sheets(sheets)


def load_sheets_as_dataframes(
    service: Resource,
    spreadsheet_id: str,
    range: str = "!A1:B1000",
    has_header_row: bool = True,
) -> Dict[SheetTitle, pd.DataFrame]:
    """Returns the contents of a spreadsheet as Pandas dataframes:
        {
            sheet_title1: content_dataframe1,
            ...
        }
    """
    sheet_titles: SheetTitles = get_sheet_titles_for_spreadsheet(
        service, spreadsheet_id
    )
    ranges = [sheet_title + range for sheet_title in sheet_titles]

    results: Dict = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id, ranges=ranges, fields="valueRanges(range,values)"
    ).execute()

    dataframes: Dict[SheetTitle, pd.DataFrame] = {}
    for result in results["valueRanges"]:
        if "values" not in result:
            continue

        range: str = result["range"]
        # the sheet title is the part of the string that comes before the "!"
        sheet_title: SheetTitle = range.split(SHEET_RANGE_SPLIT_CHAR, 1)[0]
        sheet_df: pd.DataFrame = pd.DataFrame.from_records(result["values"])

        if has_header_row:
            sheet_df.columns = sheet_df.iloc[0]
            sheet_df = sheet_df.reindex(sheet_df.index.drop(0))
        dataframes[sheet_title] = sheet_df
    return dataframes


def load_sheet_as_dataframe(sheets_service, spreadsheet_id, range, has_header_row=True):
    # Try getting the sheet - if that is not possible, return an empty dataframe
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )

    values = result.get("values", [])

    if not values:
        return None
    else:
        sheet_df = pd.DataFrame.from_records(values)
        if has_header_row:
            sheet_df.columns = sheet_df.iloc[0].str.strip()
            sheet_df = sheet_df.reindex(sheet_df.index.drop(0))
        return sheet_df


def load_doc_as_query(docs_service, document_id) -> str:
    # Retrieve the documents contents from the Docs service.
    document = docs_service.documents().get(documentId=document_id).execute()
    body = document.get("body")

    if not body:
        return None
    else:
        lines = []
        for element in body.get("content"):
            if "paragraph" in element:
                for p_element in element.get("paragraph").get("elements"):
                    if "textRun" in p_element:
                        lines.append(p_element.get("textRun").get("content").strip())
        query = " ".join(lines)
        return query


def add_sheets(
    sheets_service: Resource, sheet_titles: SheetTitles, spreadsheet_id: str
):
    """Adds sheets for the provided titles to a Google Sheet."""
    if not sheet_titles:
        return

    requests: List[Dict] = []

    for sheet_title in sheet_titles:
        requests.append({"addSheet": {"properties": {"title": sheet_title}}})

    body = {"requests": requests}

    return (
        sheets_service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute()
    )


def batch_update(sheets_service: Resource, body: Dict, spreadsheet_id: str):
    """
    Peforms a Google Sheet batch update to apply one or more updates to a spreadsheet,
    like adding data validation or creating a new sheet.
    """

    return (
        sheets_service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute()
    )


def value_batch_update(sheets_service: Resource, body: Dict, spreadsheet_id: str):
    """
    Peforms a Google Sheet values batch update to set values in one or more
    ranges of a spreadsheet.
    """

    return (
        sheets_service.spreadsheets()
        .values()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute()
    )


def value_batch_clear(sheets_service: Resource, body: Dict, spreadsheet_id: str):
    """
    Peforms a Google Sheet values batch clear to clear values in one or more
    ranges of a spreadsheet.
    """

    return (
        sheets_service.spreadsheets()
        .values()
        .batchClear(spreadsheetId=spreadsheet_id, body=body)
        .execute()
    )


def get_data_validation_request(
    sheet_id: str,
    values: List[str],
    start_column_index: int = None,
    end_column_index: int = None,
):
    """
    Creates and returns a batch update request for data validation.

    Keyword arguments:
    start_column_index -- first column to apply validation, inclusive
    end_column_index -- last column to apply validation, inclusive

    If no values are provided, an empty request is returned.
    If no start column index is provided, none will be set.
    If no end column index is provided, none will be set.
    """

    if not values:
        return {}

    return {
        "setDataValidation": {
            "range": {
                k: v
                for k, v in {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": start_column_index,
                    "endColumnIndex": end_column_index + 1
                    if end_column_index is not None
                    else None,
                }.items()
                if v is not None
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": value} for value in values],
                },
                "strict": True,
                "showCustomUi": True,
            },
        }
    }


def get_auto_resize_request(
    sheet_id: str, start_index: int = None, end_index: int = None
):
    """
    Creates and returns a batch update request for auto resizing Google sheet columns.

    Keyword arguments:
    start_index -- first column to apply resizing, inclusive
    end_index -- last column to apply resizing, inclusive

    If no start column index is provided, none will be set.
    If no end column index is provided, none will be set.
    """
    return {
        "autoResizeDimensions": {
            "dimensions": {
                k: v
                for k, v in {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": start_index,
                    "endIndex": end_index + 1 if end_index is not None else None,
                }.items()
                if v is not None
            }
        }
    }
