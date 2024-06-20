import pygsheets
import sys
import pandas as pd

SERVICE_FILE = "creds.json"
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Subprocess", "Step", "Pharmacists_Label")
SOURCE_SHEET = "Adjazenzlisten_Medication_Review"
OUTPUT_SHEET = "Medication_Review_Triples"
FIRST_SOURCE_WKS = 3


class SheetMaker(object):
    def __init__(self):
        # Google Sheets
        self.worksheet_number = 0
        self.gc = pygsheets.authorize(service_file=SERVICE_FILE)
        self.input_sheet = self.gc.open(SOURCE_SHEET)
        self.output_sheet = self.gc.open(OUTPUT_SHEET)

        # DataFrames
        self.df_in = pd.DataFrame()
        self.df_out = pd.DataFrame(columns=COLUMNS)
        self.df_nodes = pd.DataFrame()
        self.df_total = pd.DataFrame()

        # Else
        self.triple_number = 0
        self.pharmacists_label = "not set"

    def get_df(self, worksheet_number):
        wks = self.input_sheet[worksheet_number]
        self.pharmacists_label = wks.title
        self.df_in = wks.get_as_df()

    def is_not_empty(self, cell):
        if cell == "":
            return False
        else:
            return True

    def add_end(self, source, row_number):
        target = "end"
        edge = "is"
        data = [source, edge, target, row_number, self.triple_number+2, self.pharmacists_label]
        new_row = pd.DataFrame([data], columns=self.df_out.columns)
        self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)

    def add_start(self, target, row_number):
        source = "start"
        edge = "is"
        target_list = target.split(";")
        for target in target_list:
            self.triple_number += 1
            data = [source, edge, target, row_number, self.triple_number, self.pharmacists_label]
            new_row = pd.DataFrame([data], columns=self.df_out.columns)
            self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)

    def process_row(self):
        row_number = 0
        self.df_in = self.df_in.replace({" ": "", ",": "", ":": ""}, regex=True)
        # Iterate through the dataframe to add triples to the RDF graph
        for index, row in self.df_in.iterrows():
            row_number += 1
            self.triple_number = 0
            self.add_start(row.iloc[0], row_number)
            for col in range(1, len(row), 2):  # Iterate over pairs of columns (Edge, Target)
                source = row.iloc[col-1]
                edge = row.iloc[col]
                target = row.iloc[col + 1]
                if self.is_not_empty(edge) and self.is_not_empty(target):
                    source_list = source.split(";")
                    target_list = target.split(";")
                    for source in source_list:
                        for target in target_list:
                            self.triple_number += 1
                            # row_number+1, triple_number+1 because start/end
                            data = [source, edge, target, row_number, self.triple_number+1, self.pharmacists_label]
                            new_row = pd.DataFrame([data], columns=self.df_out.columns)
                            self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)
                else:
                    source_list = source.split(";")
                    for source in source_list:
                        self.add_end(source, row_number)
                    break

    def write_xlsx(self):
        worksheet = self.output_sheet.worksheet_by_title(self.pharmacists_label)
        worksheet.clear()
        self.add_nodes_to_df()
        # Write the DataFrame to the Google Sheet
        worksheet.set_dataframe(self.df_out, (1, 1))
        print("successfully wrote to google sheet")

    def add_nodes_to_df(self):
        for node_type in ["Source_Node", "Target_Node"]:
            self.df_nodes = pd.concat([self.df_nodes, self.df_out.loc[:, node_type]], ignore_index=True)
        self.df_total = pd.concat([self.df_total, self.df_out], ignore_index=True)

    def write_nodes_sheet(self):
        worksheet = self.output_sheet.worksheet_by_title("Nodes")
        self.df_nodes = self.df_nodes.drop_duplicates(keep="first")
        worksheet.set_dataframe(self.df_nodes, (1, 1))
        print("successfully wrote Nodes to google sheet")
        worksheet = self.output_sheet.worksheet_by_title("Total")
        worksheet.set_dataframe(self.df_total, (1, 1))
        print("successfully wrote Total google sheet")



def main():
    count = 0
    maker = SheetMaker()
    for worksheet_number in range(FIRST_SOURCE_WKS, 8):
        maker.get_df(worksheet_number)
        maker.process_row()
        maker.write_xlsx()
        print(f"Processed {count+1}.Sheet")
        count += 1
    maker.write_nodes_sheet()
    return 0


sys.exit(main())
