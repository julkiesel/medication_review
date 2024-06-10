import pygsheets
import sys
import pandas as pd
from os import path

SERVICE_FILE = "creds.json"
SOURCE_SHEET = "Medication_Review_Triples"
DIR_OUT = path.abspath("CSV")


class SheetMaker(object):
    def __init__(self, google_sheet):
        self.worksheet_number = 0
        self.gc = pygsheets.authorize(service_file=SERVICE_FILE)
        self.sh = self.gc.open(google_sheet)
        self.df = pd.DataFrame()
        self.pharmacists_label = "not set"

    def get_df(self, worksheet_number):
        wks = self.sh[worksheet_number]
        self.pharmacists_label = wks.title
        self.df = wks.get_as_df()

    def write_csv(self):
        self.df.to_csv(path.join(DIR_OUT, self.pharmacists_label + ".csv"), quotechar='"', index=False)

def main():
    maker = SheetMaker(SOURCE_SHEET)
    for worksheet_number in range(0, 5):
        maker.get_df(worksheet_number)
        maker.write_csv()
        print(f"Processed {worksheet_number+1}.Sheet")

    return 0


sys.exit(main())