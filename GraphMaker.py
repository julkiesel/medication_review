import pygsheets
import pandas as pd
import sys
from dataclasses import dataclass
from typing import Optional

SERVICE_FILE = "creds.json"
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Subprocess", "Step", "Pharmacists_Label")
SOURCE_SHEET = "Adjazenzlisten_Medication_Review"
OUTPUT_SHEET = "Medication_Review_Triples"
PHARMACISTS = ["Pharmacist_1", "Pharmacist_2", "Pharmacist_3", "Pharmacist_4", "Pharmacist_5"]
NODES_SHEET = "Nodes"

class SheetMaker(object):
    @dataclass
    class Triple:
        row_number: int
        triple_number: int
        source: Optional[str] = None
        edge: Optional[str] = None
        target: Optional[str] = None

    def __init__(self):
        self.sheet = self.initiate_sheet(SOURCE_SHEET)
        self.output_sheet = self.initiate_sheet(OUTPUT_SHEET)
        self.nodes_sheet = self.initiate_sheet(NODES_SHEET)
        self.df_in = pd.DataFrame()
        self.df_out = pd.DataFrame(columns=COLUMNS)
        self.df_total = pd.DataFrame(columns=COLUMNS)
        self.df_nodes = self.nodes_sheet.worksheet_by_title(NODES_SHEET).get_as_df()
        self.pharmacist = "not set"

    def initiate_sheet(self, google_sheet):
        gc = pygsheets.authorize(service_file=SERVICE_FILE)
        sh = gc.open(google_sheet)
        return sh

    def get_df(self, title):
        wks = self.sheet.worksheet_by_title(title)
        self.df_in = wks.get_as_df()
        self.pharmacist = wks.title

    def is_not_empty(self, cell):
        if cell == "":
            return False
        else:
            return True
    def is_an_outcome(self, edge):
        if edge in ["needs", "needsRequestOf", "needsClarificationOf", "hasOutcome", "giveProposalOf"]:
            return True
        else:
            return False
    def is_part_of_epa(self, target):
        try:
            target_node_filter = self.df_nodes["Name"] == target
            target_node_data = self.df_nodes.loc[target_node_filter, "node_of_interest"]
            target_value = target_node_data.values[0]
        except Exception as e:
            print(f"Error with target: {target} and message: {e}")
            target_value = target
        if pd.isna(target_value):
            return False    # it should go
        else:
            return True

    def modify_triple(self, triple):
        if self.is_an_outcome(triple.edge):
            return triple
        else:
            target_list = triple.target.split(";")
            new_target_list = []
            for target in target_list:
                if self.is_part_of_epa(target):
                    new_target_list.append(target)
            triple.target = ";".join(new_target_list)
            return triple

    def add_triple_to_df(self, triple):
        source_list = triple.source.split(";")
        target_list = triple.target.split(";")
        for source in source_list:
            for target in target_list:
                data = [source, triple.edge, target, triple.row_number, triple.triple_number, self.pharmacist]
                new_row = pd.DataFrame( [data], columns=self.df_out.columns)
                self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)

    def add_end(self, triple):
        triple.target = "end"
        triple.edge = "is"
        self.add_triple_to_df(triple)

    def add_start(self, triple):
        triple.source = "start"
        triple.edge = "is"
        self.add_triple_to_df(triple)

    def process_df(self, epa_style):
        self.df_in = self.df_in.replace({" ": "", ",": "", ":": ""}, regex=True)
        self.df_out = pd.DataFrame(columns=COLUMNS)
        row_number = 0
        for index, row in self.df_in.iterrows():
            row_number += 1
            if epa_style:
                self.process_row_epa_style(row_number, row)
            else:
                self.process_row_normal(row_number, row)
        self.write_xlsx()

    def process_row_normal(self, row_number, row):
        triple_number = 1
        triple = self.Triple(row_number, triple_number, None, None, row.iloc[0])
        self.add_start(triple)
        for col in range(1, len(row), 2):  # Iterate over pairs of columns (Edge, Target)
            triple_number += 1
            source = row.iloc[col - 1]
            edge = row.iloc[col]
            target = row.iloc[col + 1]
            triple = self.Triple(row_number, triple_number, source, edge, target)
            if self.is_not_empty(edge) and self.is_not_empty(target):
                self.add_triple_to_df(triple)
            else:   # the end of the row
                self.add_end(triple)
                break
            # Longest row in the dataframe needs end node
            if col == len(row) - 2:
                source = row.iloc[col + 1]
                if self.is_not_empty(source):
                    triple_number += 1
                    triple = self.Triple(row_number, triple_number, source)
                    self.add_end(triple)
                break
    def process_row_epa_style(self, row_number, row):
        triple_number = 1
        source_pos = -1
        triple = self.Triple(row_number, triple_number, None, None, row.iloc[0])
        self.add_start(triple)
        for col in range(1, len(row)+1, 2):  # Iterate over pairs of columns (Edge, Target)
            triple_number += 1
            edge = row.iloc[col]
            target = row.iloc[col + 1]
            if source_pos == -1:
                source = row.iloc[col - 1]
            else:
                source = row.iloc[source_pos]
                if not self.is_an_outcome(edge):
                    edge = "connectedTo"
            triple = self.Triple(row_number, triple_number, source, edge, target)
            if self.is_not_empty(edge) and self.is_not_empty(target):
                triple = self.modify_triple(triple)
                row.iloc[col + 1] = triple.target
                if self.is_not_empty(triple.target):
                    self.add_triple_to_df(triple)
                    source_pos = -1
                else:
                    if source_pos == -1:
                        source_pos = col - 1
            else:   # the end of the row
                self.add_end(triple)
                break
            # Longest row in the dataframe needs end node
            if col == len(row) - 2:
                source = row.iloc[col + 1]
                if self.is_not_empty(source):
                    triple_number += 1
                    triple = self.Triple(row_number, triple_number, source)
                    self.add_end(triple)
                break

    def write_xlsx(self):
        try:
            worksheet = self.output_sheet.worksheet_by_title(self.pharmacist)
        except pygsheets.exceptions.WorksheetNotFound:
            worksheet = self.output_sheet.add_worksheet(self.pharmacist)
        worksheet.clear()
        self.add_total_df()
        # Write the DataFrame to the Google Sheet
        worksheet.set_dataframe(self.df_out, (1, 1))
        print("successfully wrote to google sheet")

    def add_total_df(self):
        self.df_total = pd.concat([self.df_total, self.df_out], ignore_index=True)

    def write_total_sheet(self):
        worksheet = self.output_sheet.worksheet_by_title("Total")
        worksheet.clear()
        worksheet.set_dataframe(self.df_total, (1, 1))
        print("successfully wrote Total google sheet")


def main():
    count = 0
    process_in_epa_style = False
    maker = SheetMaker()
    for title in PHARMACISTS:
        maker.get_df(title)
        maker.process_df(process_in_epa_style)
        print(f"Processed {count + 1}.Sheet")
        count += 1
    maker.write_total_sheet()
    return 0


sys.exit(main())
