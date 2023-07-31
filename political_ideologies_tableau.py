import json
import os
from csv import reader
from datetime import datetime
from googleapiclient import discovery
from google.oauth2 import service_account
from requests import get
from data_pipeline import DataPipeline


def main():
    # Setup Google API service and authorization
    scopes = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/drive.file",
              "https://www.googleapis.com/auth/spreadsheets"]
    secret_file = json.loads(os.environ.get('GOOGLE_AUTHENTICATION_CREDENTIALS'))
    spreadsheet_id = os.environ.get('GOOGLE_SPREADSHEET_URL')

    credentials = service_account.Credentials.from_service_account_info(secret_file, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    # Metadata--Current Congress and relevant CSV URL
    hs_code = 118 + int((datetime.now() - datetime(2023, 1, 3, 12)).days / 730)
    url = f"https://voteview.com/static/data/out/members/HS{hs_code}_members.csv"

    with get(url, stream=True) as r:
        rows = reader(line.decode('utf-8') for line in r.iter_lines())
        next(rows)  # the first row in the data is column names

        # Initialize request
        sheet_row_num = 49945  # epoch is the end of the 117th Congress

        pipeline = DataPipeline(service, spreadsheet_id, None, current_row=sheet_row_num)

        # find current congress
        pipeline.find_current_congress(hs_code)

        # Finally update the database
        pipeline.update_data(rows)


if __name__ == '__main__':
    main()
