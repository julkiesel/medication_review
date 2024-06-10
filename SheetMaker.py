import pygsheets
import sys
import pandas as pd

SERVICE_FILE = "creds.json"
COLUMNS = ("Subject", "Predicate", "Object", "Row_Number", "Triple_Number", "Pharmacists_Label")
SOURCE_SHEET = "Graph des Medication Review"
OUTPUT_SHEET = "Medication_Review_Triples"
FIRST_SOURCE_WKS = 3


class SheetMaker(object):
    def __init__(self, google_sheet):
        self.worksheet_number = 0
        self.gc = pygsheets.authorize(service_file=SERVICE_FILE)
        self.sh = self.gc.open(google_sheet)
        self.df = pd.DataFrame()
        self.modified_df = pd.DataFrame(columns=COLUMNS)
        self.pharmacists_label = "not set"

    def get_df(self, worksheet_number):
        wks = self.sh[worksheet_number]
        self.pharmacists_label = wks.title
        self.df = wks.get_as_df()
        self.modified_df = pd.DataFrame(columns=COLUMNS)

    def is_not_empty(self, cell):
        if cell == "":
            return False
        else:
            return True

    def process_row(self):
        row_number = 0
        self.df = self.df.replace({" ": "", ",": "", ":": ""}, regex=True)
        # Iterate through the dataframe to add triples to the RDF graph
        for index, row in self.df.iterrows():
            row_number += 1
            triple_number = 0
            for col in range(1, len(row), 2):  # Iterate over pairs of columns (Edge, Target)
                source = row.iloc[col-1]
                edge = row.iloc[col]
                target = row.iloc[col + 1]
                if self.is_not_empty(edge) and self.is_not_empty(target):
                    source_list = source.split(";")
                    target_list = target.split(";")
                    for source in source_list:
                        for target in target_list:
                            triple_number += 1
                            data = [source, edge, target, row_number, triple_number, self.pharmacists_label]
                            new_row = pd.DataFrame([data], columns=self.modified_df.columns)
                            self.modified_df = pd.concat([self.modified_df, new_row], ignore_index=True)
                else:
                    break

    def write_xlsx(self, number):
        spreadsheet = self.gc.open(OUTPUT_SHEET)
        worksheet = spreadsheet[number]
        worksheet.clear()
        worksheet.title = self.pharmacists_label
        # Write the DataFrame to the Google Sheet
        worksheet.set_dataframe(self.modified_df, (1, 1))
        print("successfully wrote to google sheet")


def main():
    count = 0
    maker = SheetMaker(SOURCE_SHEET)
    for worksheet_number in range(FIRST_SOURCE_WKS, 8):
        maker.get_df(worksheet_number)
        maker.process_row()
        maker.write_xlsx(count)
        print(f"Processed {count+1}.Sheet")
        count += 1
    return 0


sys.exit(main())
