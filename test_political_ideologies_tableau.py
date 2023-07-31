import random as rand
import os
import pytest
from csv import reader, writer
from database_ops import DatabaseOps
from data_pipeline import DataPipeline

# Metadata
test_data_path = 'test_data'
test_data_filenames = ['HS117_members.csv', 'HS118_members.csv']
wacky_data_filenames = ['HS117_members_wackified.csv', 'HS118_members_wackified.csv']
test_data_lengths = [559, 541]
test_data_offset = 25  # Prevents running over quota limit
current_congress = 118
# Set up environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'pol-ideologies-tableau-test-3caeaab90fff.json'


# Converts approximately half the data to random floats
def create_wacky_data(source_filenames, target_filenames, max_rows=None):
    for source, target in zip(source_filenames, target_filenames):
        rows = get_array_from_csv(source, max_rows)
        target_writer = writer(open(os.path.join(test_data_path, target), 'w'))

        for row in rows:
            for i in range(len(row)):
                if rand.random() < 0.5:
                    row[i] = round(rand.random(), ndigits=5)

        target_writer.writerows(rows)


# Converts all strings containing numerical values into a numerical representation.
def numericalize(rows):
    for row in rows:
        for i in range(len(row)):
            try:
                row[i] = float(row[i])
            except ValueError:
                pass


def strip_trailing_empty_strings(rows):
    for row in rows:
        while row[-1] == '':
            del row[-1]


def get_json_from_rows(data_range: str, rows: list[list[str]]) -> dict[str, any]:
    return {'range': data_range, 'majorDimension': 'ROWS', 'values': rows}


def get_array_from_csv(filename, max_rows=None):
    file = reader(open(os.path.join(test_data_path, filename), 'r'))
    return list(file)[:max_rows]


class TestPoliticalIdeologiesTableau(DatabaseOps):

    def assert_spreadsheet_equals_data(self, data_filenames, check_length=False):
        request = self.sheets_service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range='Sheet1',
                                                                  valueRenderOption='UNFORMATTED_VALUE')
        result = request.execute()
        values = result['values']

        combined_data = []
        for filename in data_filenames:
            combined_data += get_array_from_csv(filename)

        assert (not check_length) or (len(values) == len(combined_data))

        strip_trailing_empty_strings(combined_data)
        numericalize(values)
        numericalize(combined_data)

        for row1, row2 in zip(values, combined_data):
            assert row1 == row2

    def setup_database(self, filenames, max_rows_list=None):
        start_row = 1

        if max_rows_list is None:
            max_rows_list = [None] * len(filenames)

        for filename, max_rows in zip(filenames, max_rows_list):
            rows = get_array_from_csv(filename, max_rows)
            end_row = start_row + len(rows) - 1
            data_range = f'Sheet1!R{start_row}C1:R{end_row}C{len(rows[0])}'
            print(f'END ROW: {end_row}')
            start_row = end_row + 1
            self.sheets_service.spreadsheets().values().append(spreadsheetId=self.spreadsheet_id, range=data_range,
                                                               valueInputOption="USER_ENTERED",
                                                               body=get_json_from_rows(data_range, rows)).execute()

    def test_create_database(self):
        self.create_database()
        # TODO compare created database to CSV

    def test_delete_database(self):
        self.create_database()
        self.delete_database()
        # TODO Ensure it's gone

    def test_connect_to_database(self):
        self.create_database()
        request = self.sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id)
        request.execute()
        self.delete_database()

    def test_setup_database(self):
        self.create_database()
        self.setup_database(test_data_filenames)
        self.assert_spreadsheet_equals_data(test_data_filenames)
        self.delete_database()

    def test_find_current_congress(self):
        self.create_database()
        self.setup_database(test_data_filenames)

        expected_index = test_data_lengths[0] + 1
        pipeline = DataPipeline(service=self.sheets_service, spreadsheet_id=self.spreadsheet_id,
                                current_congress_dne=None, current_row=expected_index - test_data_offset)
        pipeline.find_current_congress(current_congress)
        assert pipeline.current_row == expected_index

        expected_index = sum(test_data_lengths) + 1
        pipeline.current_row = expected_index - test_data_offset
        pipeline.find_current_congress(hs_code=119)
        # Since the congress is not present, it should go to the end of the table
        assert pipeline.current_row == expected_index

        self.delete_database()

    def test_append_congress(self):
        self.create_database()
        self.setup_database([test_data_filenames[0]])
        pipeline = DataPipeline(service=self.sheets_service, spreadsheet_id=self.spreadsheet_id,
                                current_congress_dne=True, current_row=test_data_lengths[0] + 1)
        # append new congress to database
        pipeline.update_data(rows=get_array_from_csv(test_data_filenames[1], max_rows=10))
        self.assert_spreadsheet_equals_data(test_data_filenames)
        self.delete_database()

    def test_update_congress(self):
        num_rows_to_update = 10
        self.create_database()
        self.setup_database(test_data_filenames, max_rows_list=[None, num_rows_to_update])
        pipeline = DataPipeline(service=self.sheets_service, spreadsheet_id=self.spreadsheet_id,
                                current_congress_dne=False, current_row=test_data_lengths[0] + 1)

        # update entries (same length)
        create_wacky_data(test_data_filenames, wacky_data_filenames, max_rows=num_rows_to_update)
        pipeline.update_data(rows=get_array_from_csv(wacky_data_filenames[1], max_rows=num_rows_to_update))
        self.assert_spreadsheet_equals_data([test_data_filenames[0], wacky_data_filenames[1]])

        # update entries (new data longer)
        num_rows_to_update = 13
        create_wacky_data(test_data_filenames, wacky_data_filenames, max_rows=num_rows_to_update)
        pipeline.current_row = test_data_lengths[0] + 1
        pipeline.update_data(rows=get_array_from_csv(wacky_data_filenames[1], max_rows=num_rows_to_update))
        self.assert_spreadsheet_equals_data([test_data_filenames[0], wacky_data_filenames[1]])

        # update entries (new data shorter)
        num_rows_to_update = 5
        create_wacky_data(test_data_filenames, wacky_data_filenames, max_rows=num_rows_to_update)
        pipeline.current_row = test_data_lengths[0] + 1
        pipeline.update_data(rows=get_array_from_csv(wacky_data_filenames[1], max_rows=num_rows_to_update))
        self.assert_spreadsheet_equals_data([test_data_filenames[0], wacky_data_filenames[1]], check_length=True)

        self.delete_database()


if __name__ == '__main__':
    pytest.console_main()
