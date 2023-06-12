import datetime
import re
from pathlib import Path

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from .models import SheetState, Campaign
from .utils import env, now, retry_with_backoff

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = env.str("SPREADSHEET_ID")
TOKEN_PATH = env.str("TOKEN_PATH", default="token.json")
grid_limit_regex_error: re.Pattern = re.compile(
    r".+Range \('(?P<sheet_name>[\w-]+)'!\w+\) exceeds grid limits. Max rows: (?P<rows>\d+), max columns: \d+"
)


def get_sheet_service():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if Path(TOKEN_PATH).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(TOKEN_PATH, SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


def get_sheet_max_row(service, sheet_name):
    properties = retry_with_backoff(
        lambda: (
            service.spreadsheets()
            .get(
                spreadsheetId=SPREADSHEET_ID,
                ranges=sheet_name,
                fields="sheets(properties(gridProperties(columnCount,rowCount)))",
            )
            .execute()
        ),
        backoff_in_seconds=10,
    )
    return properties["sheets"][0]["properties"]["gridProperties"]["rowCount"]


def get_sheet_state(service):
    batch_size = 100
    start_row = 1
    max_row = get_sheet_max_row(service, "Darty-campaign")
    state = SheetState()
    while result := retry_with_backoff(
        lambda: (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"Darty-campaign!A{start_row}:W{min(start_row + batch_size, max_row)}",
            )
            .execute()
            .get("values", [])
        ),
        backoff_in_seconds=10,
    ):
        if start_row == 1:
            state.has_headers = True
        state.last_campaign_date = datetime.date.fromisoformat(result[-1][0])

        start_row += len(result)
        if start_row >= max_row:
            break
    if state.has_headers is None:
        state.has_headers = False
    else:
        state.new_rows["campaign"] = start_row
    if state.last_campaign_date is not None:
        month_start = now.date().replace(day=1)
        if state.last_campaign_date >= month_start:
            time_diff = now.date() - state.last_campaign_date
            if time_diff.days < 1:
                state.campaign_period = "Day"
            if time_diff.days == 1:
                state.campaign_period = "Week"
            else:
                state.campaign_period = "Month"
        # else keep default "Year" period

    return state


def update_sheet_state(service, state: SheetState, campaigns: list[Campaign]):
    # if we don't have headers, that means the doc is empty
    if not state.has_headers:
        return
    campaign_titles = set(campaign.title for campaign in campaigns)
    batch_size = 100
    for data_type, last_id_column in [
        ("link", "B"),
        ("user_agent", "C"),
        ("region", "B"),
    ]:
        start_row = 2  # jump headers
        max_row = get_sheet_max_row(service, f"Darty-{data_type}")
        while result := (
            retry_with_backoff(
                lambda: (
                    service.spreadsheets()
                    .values()
                    .get(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"Darty-{data_type}!A{start_row}:{last_id_column}{min(start_row + batch_size, max_row)}",
                    )
                    .execute()
                    .get("values", [])
                ),
                backoff_in_seconds=10,
            )
        ):
            for index, row in enumerate(result):
                if row[0] in campaign_titles:
                    getattr(state, f"{data_type}_mapping")[tuple(row)] = (
                        index + start_row
                    )

            start_row += len(result)
            if start_row >= max_row:
                break
        state.new_rows[data_type] = start_row


def get_sheet_ids(spreadsheets) -> dict:
    properties = retry_with_backoff(
        lambda: (
            spreadsheets.get(
                fields="sheets(properties(title,sheetId))",
                spreadsheetId=SPREADSHEET_ID,
            ).execute()
        ),
        backoff_in_seconds=10,
    )
    return {
        sheet["properties"]["title"]: sheet["properties"]["sheetId"]
        for sheet in properties["sheets"]
    }


def try_expand_batch_update(spreadsheets, sheet_ids: dict, body: dict):
    try:
        spreadsheets.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body
        ).execute()
    except HttpError as e:
        match = grid_limit_regex_error.match(e.reason)
        if match is not None:
            retry_with_backoff(
                lambda: (
                    spreadsheets.batchUpdate(
                        spreadsheetId=SPREADSHEET_ID,
                        body={
                            "requests": [
                                {
                                    "appendDimension": {
                                        "sheetId": sheet_ids[match.group("sheet_name")],
                                        "dimension": "ROWS",
                                        "length": 1000,
                                    }
                                },
                            ]
                        },
                    ).execute()
                ),
                backoff_in_seconds=10,
            )

            try_expand_batch_update(spreadsheets, sheet_ids, body)
        else:
            raise e


def write_data_to_sheet(service, data: list):
    # Call the Sheets API
    spreadsheets = service.spreadsheets()
    sheet_ids = get_sheet_ids(spreadsheets)
    batch_size = 100
    for i in range(0, len(data), batch_size):
        body = {"valueInputOption": "RAW", "data": data[i : i + batch_size]}
        retry_with_backoff(
            lambda: try_expand_batch_update(spreadsheets, sheet_ids, body),
            backoff_in_seconds=10,
        )
