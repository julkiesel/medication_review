"""
1) Zugriff auf Google Sheets, neues Tabellenblatt
2) Tabellenblatt: Node, Edge, Target, Pharmacist1, Pharmacist2, Pharmacist3, Pharmacist4, Pharmacist5
3) Sammele Daten:
- Iteriere über Tabellenblatt Total
- wenn in Spalte Edge
a) hasOutcome
b) needs, needsClarification
c) needsRequestOf
- dann prüfe ob Zeile schon vorhanden, dann ergänze bei Pharmacist x
- wenn nicht vorhanden, dann füge Zeile hinzu

4) Eintragung von Daten in das Tabellenblatt je Pharmazeut in {row_numbers}

TripleTree:
    enthält alle Triples
    ermöglicht Zugriff auf Triples nach source, edge, target
    Value ist Triple selbst

class Triple:
    source: Optional[str] = None
    edge: Optional[str] = None
    target: Optional[str] = None
    pharmacist_1: [int] Liste an Row_Numbers



"""
import dataclasses
import pygsheets
import pandas as pd
import sys


# Constants
SERVICE_FILE = "../creds.json"
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Pharmacist_1", "Pharmacist_2", "Pharmacist_3", "Pharmacist_4", "Pharmacist_5", "Count")
SOURCE_SHEET = "Medication_Review_Triples"
SOURCE_WORKSHEET = "Total"
OUTPUT_SHEET = "Evaluation"
OUTPUT_WORKSHEET = "Outcome_Comparism"
# Columns in Source Sheet
PHARMACIST = "Pharmacists_Label"
ROW = "Subprocess"

class Evaluator:
    @dataclasses.dataclass(frozen=True)
    class Triple:
        source: str
        edge: str
        target: str

    def __init__(self):
        self.source_data = self.load_data(SOURCE_SHEET, SOURCE_WORKSHEET)
        self.output_data = pd.DataFrame(columns=COLUMNS)
        self.outcome_organizer = {}

    def load_data(self, google_sheet, worksheet_title):
        gc = pygsheets.authorize(service_file=SERVICE_FILE)
        sh = gc.open(google_sheet)
        ws = sh.worksheet_by_title(worksheet_title)
        data = ws.get_as_df()
        return data

    def clean_data(self):
        for index, row in self.source_data.iterrows():
            if not self.is_an_outcome(row["Relationship"]):
                self.source_data.drop(index, inplace=True)

    def write_organizer(self):
        for index, row in self.source_data.iterrows():
            if row["Relationship"] == "needsRequestOf":
                row["Source_Node"] = ""
            triple = self.Triple(row["Source_Node"], row["Relationship"], row["Target_Node"])
            if triple in self.outcome_organizer.keys():
                self.outcome_organizer[triple].append([row[PHARMACIST], row[ROW]])
            else:
                occurrences = [[row[PHARMACIST], row[ROW]]]
                self.outcome_organizer[triple] = occurrences

    def write_output_data(self):
        for triple, occurrences in self.outcome_organizer.items():
            pharmacist_1 = []
            pharmacist_2 = []
            pharmacist_3 = []
            pharmacist_4 = []
            pharmacist_5 = []
            count = 5
            for occurrence in occurrences:
                if occurrence[0] == "Pharmacist_1":
                    pharmacist_1.append(occurrence[1])
                elif occurrence[0] == "Pharmacist_2":
                    pharmacist_2.append(occurrence[1])
                elif occurrence[0] == "Pharmacist_3":
                    pharmacist_3.append(occurrence[1])
                elif occurrence[0] == "Pharmacist_4":
                    pharmacist_4.append(occurrence[1])
                elif occurrence[0] == "Pharmacist_5":
                    pharmacist_5.append(occurrence[1])
            row = [triple.source, triple.edge, triple.target, pharmacist_1, pharmacist_2, pharmacist_3, pharmacist_4,
                   pharmacist_5]
            for i in range(3, 8):
                if len(row[i]) == 0:
                    count -= 1
            row.append(count)
            new_row = pd.DataFrame([row], columns=COLUMNS)
            self.output_data = pd.concat([self.output_data, new_row], ignore_index=True)

    def is_an_outcome(self, edge):
        if edge in ["needs", "needsRequestOf", "needsClarificationOf", "hasOutcome", "giveProposalOf", "needsResearchIn"]:
            return True
        else:
            return False

    def process_data(self):
        self.clean_data()
        self.write_organizer()
        self.write_output_data()
        self.write_to_google_sheet()

    def write_to_google_sheet(self):
        gc = pygsheets.authorize(service_file=SERVICE_FILE)
        sh = gc.open(OUTPUT_SHEET)
        ws = sh.worksheet_by_title(OUTPUT_WORKSHEET)
        ws.clear()
        ws.set_dataframe(self.output_data, (1, 1))
        print("successfully wrote to google sheet")

def main():
    evaluator = Evaluator()
    evaluator.process_data()

    return 0

sys.exit(main())
