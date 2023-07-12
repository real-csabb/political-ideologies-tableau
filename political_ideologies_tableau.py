import json
import googleapiclient
import os
from csv import reader
from datetime import datetime
from time import sleep

from apiclient import discovery
from google.oauth2 import service_account
from requests import get

# Setup Google API service and authorization
scopes = ["https://www.googleapis.com/auth/drive",
          "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/spreadsheets"]
secret_file = json.loads(os.environ.get('GOOGLE_AUTHENTICATION_CREDENTIALS'))
spreadsheet_id = os.environ.get('GOOGLE_SPREADSHEET_URL')

credentials = service_account.Credentials.from_service_account_info(secret_file, scopes=scopes)
service = discovery.build('sheets', 'v4', credentials=credentials)

def main():
    # Metadata--Current Congress and relevant CSV URL
    hs_code = 118 + int((datetime.now() - datetime(2023, 1, 3, 12)).days / 730)
    url = f"https://voteview.com/static/data/out/members/HS{hs_code}_members.csv"

    with get(url, stream=True) as r:
        rows = reader(line.decode('utf-8') for line in r.iter_lines())
        next(rows)  # the first row in the data is column names

        # Initialize request
        sheet_row_num = 49945  # epoch is the end of the 117th Congress
        current_row = f'data!R{sheet_row_num}C1:R{sheet_row_num}C22'
        request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=current_row)
        result = request.execute()['values'][0][0]

        # Find which row the current Congress begins
        current_congress_dne = False
        while int(result) != hs_code:
            # update the request to be current
            sheet_row_num += 1
            current = update_row(sheet_row_num)
            request = update_getter(current)

            try:  # while not current Congress, get next row's Congress ID
                result = request.execute()['values'][0][0]

            except:  # current Congress does not exist in database (googleapiclient.errors.HttpError)
                current_congress_dne = True  # and would throw an HttpError if we tried to access it
                break

        # Finally update the database
        for row in rows:
            sleep(1)  # I'm a poor college student therefore I'm using a free GCP account
            current_row = update_row(sheet_row_num)
            row_data = update_body(current_row, row)  # like {'range': int, 'majorDimension': 'ROWS', 'values': list}

            try:
                if current_congress_dne:  # Appends new Congress to database
                    request = update_append(current_row, row_data)
                    request = request.execute()

                else:  # Update Congress
                    request = update_squared(current_row, row_data)
                    request = request.execute()
            except: # Runs where database data is incomplete
                current_congress_dne = True
                request = update_append(current_row, row_data)
                request = request.execute()

            sheet_row_num += 1  # Update row
            print(sheet_row_num)  # Debugging

def update_row(sheet_row_num: int) -> str:
    return f'data!R{sheet_row_num}C1:R{sheet_row_num}C22'

def update_getter(current_row: str) -> googleapiclient.http.HttpRequest:
    return service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=current_row)

def update_body(current_row: str, row_data: list[int]) -> dict[str, any]:
    return {'range': current_row, 'majorDimension': 'ROWS', 'values': [row_data]}

def update_append(current_row: str, row_data: list[int]) -> googleapiclient.http.HttpRequest:
    return service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=current_row, valueInputOption="USER_ENTERED", body=row_data)

def update_squared(current_row: str, row_data: list[int]) -> googleapiclient.http.HttpRequest:
    return service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=current_row, valueInputOption="USER_ENTERED", body=row_data)

if __name__ == '__main__':
    main()