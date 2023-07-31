from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth


class DatabaseOps:
    database_path = 'data.csv'
    spreadsheet_title = 'bleh'
    folder_id = '1FRqe5l3s6XYpshmPrlOp-UyGHaaVL_7U'
    spreadsheet_id = None
    sheets_service = None
    drive_service = None

    # TODO creates new database with config
    def create_database(self):
        creds, _ = google.auth.default()

        try:
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            file = {
                'name': self.spreadsheet_title,
                'parents': [self.folder_id],
                'mimeType': 'application/vnd.google-apps.spreadsheet'
            }
            request = self.drive_service.files().create(body=file)
            result = request.execute()
            self.spreadsheet_id = result['id']
            print(result)
            # spreadsheet = {
            #     'properties': {
            #         'title': self.spreadsheet_title
            #     }
            # }
            # request = self.service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId')
            # spreadsheet = request.execute()
            #print(spreadsheet)
            # self.spreadsheet_id = spreadsheet['spreadsheetId']
            #print(self.spreadsheet_id)

            # Set permissions
            # self.service.
        except HttpError as error:
            print(f'An error has occurred: {error}')
            return error

    # TODO deletes database
    def delete_database(self):
        self.drive_service.files().delete(fileId=self.spreadsheet_id).execute()
