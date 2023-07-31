from time import sleep
from googleapiclient.http import HttpRequest
from googleapiclient.errors import HttpError


class DataPipeline:

    def __init__(self, service, spreadsheet_id, current_congress_dne, sheet_name='Sheet1', sheet_id=0, current_row=1):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.current_congress_dne = current_congress_dne
        self.sheet_name = sheet_name
        self.sheet_id = sheet_id
        self.current_row = current_row

    def find_current_congress(self, hs_code):
        # Initialize request
        request = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=self.update_row())
        congress = request.execute()['values'][0][0]
        self.current_congress_dne = False

        # Find which row the current Congress begins
        while int(congress) != hs_code:
            # update the request to be current
            self.current_row += 1
            print(congress)
            print(f'code: {hs_code}')
            request = self.update_getter()

            try:
                congress = request.execute()['values'][0][0]
            except HttpError:
                self.current_congress_dne = True
                break

    def update_data(self, rows):
        for row in rows:
            sleep(1.5)  # I'm a poor college student therefore I'm using a free GCP account
            row_data = self.update_body(row)  # like {'range': int, 'majorDimension': 'ROWS', 'values': list}

            try:
                if self.current_congress_dne:  # Appends new Congress to database
                    request = self.update_append(row_data)
                    request.execute()

                else:  # Update Congress
                    request = self.update_squared(row_data)
                    request.execute()
            except HttpError:  # Runs where database data is incomplete
                current_congress_dne = True
                request = self.update_append(row_data)
                request.execute()

            self.current_row += 1  # Update row

        print(f'Start of rows to delete: {self.current_row}')
        # Get rid of trailing rows
        self.delete_trailing_rows()

    def update_row(self) -> str:
        return f'{self.sheet_name}!R{self.current_row}C1:R{self.current_row}C22'

    def update_getter(self) -> HttpRequest:
        return self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=self.update_row())

    def update_body(self, row_data: list[int]) -> dict[str, any]:
        return {'range': self.update_row(), 'majorDimension': 'ROWS', 'values': [row_data]}

    def update_append(self, row_data: list[int]) -> HttpRequest:
        return self.service.spreadsheets().values().append(spreadsheetId=self.spreadsheet_id, range=self.update_row(),
                                                           valueInputOption="USER_ENTERED", body=row_data)

    def update_squared(self, row_data: list[int]) -> HttpRequest:
        return self.service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id, range=self.update_row(),
                                                           valueInputOption="USER_ENTERED", body=row_data)

    def delete_trailing_rows(self):
        try:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    'requests': [{
                        'deleteDimension': {
                            "range": {
                                "sheetId": self.sheet_id,
                                "dimension": "ROWS",
                                "startIndex": self.current_row - 1  # This request is zero-indexed, unlike normal
                            }
                        }
                    }]
                }
            ).execute()
        # If there's no row to delete, do nothing
        except HttpError:
            pass
