"""
SheetMaker.py

Dieses Modul konvertiert Adjazenzlisten aus Google Sheets in Triple-Strukturen.

Hauptfunktionen:
- Laden von Adjazenzlisten aus Google Sheets
- Konvertierung in Triple-Strukturen (Source_Node, Relationship, Target_Node)
- Sammlung aller eindeutigen Nodes
- Export nach Google Sheets

Dies ist eine einfachere Version ohne Start/End-Nodes.

"""

import pygsheets
import sys
import pandas as pd

# Konstanten
SERVICE_FILE = "../creds.json"  # Pfad zur Google Service Account Datei
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Subprocess", "Step", "Pharmacists_Label")  # Spalten im Output
SOURCE_SHEET = "Adjazenzlisten_Medication_Review"  # Quell-Google Sheet
OUTPUT_SHEET = "Medication_Review_Triples"  # Ziel-Google Sheet
FIRST_SOURCE_WKS = 3  # Erstes zu verarbeitendes Arbeitsblatt


class SheetMaker(object):
    """
    Klasse zur Konvertierung von Adjazenzlisten in Triple-Strukturen.

    Diese Klasse lädt Adjazenzlisten und exportiert sie als Triples.

    Attributes:
        worksheet_number (int): Nummer des aktuellen Arbeitsblatts
        gc: Google Sheets Client-Objekt
        sh: Spreadsheet-Objekt
        df (pd.DataFrame): Input DataFrame mit Adjazenzliste
        modified_df (pd.DataFrame): Output DataFrame mit Triples
        pharmacists_label (str): Label des aktuellen Pharmazeuten
        intermediate_df (pd.DataFrame): Temporärer DataFrame für Nodes
        nodes_df (pd.DataFrame): DataFrame mit allen gesammelten Nodes
    """
    def __init__(self, google_sheet):
        """
        Initialisiert den SheetMaker mit einem Google Sheet.

        Args:
            google_sheet (str): Name des Google Sheets
        """
        self.worksheet_number = 0
        self.gc = pygsheets.authorize(service_file=SERVICE_FILE)
        self.sh = self.gc.open(google_sheet)
        self.df = pd.DataFrame()
        self.modified_df = pd.DataFrame(columns=COLUMNS)
        self.pharmacists_label = "not set"
        self.intermediate_df = pd.DataFrame()
        self.nodes_df = pd.DataFrame()

    def get_df(self, worksheet_number):
        """
        Lädt Daten aus einem Arbeitsblatt in einen DataFrame.

        Args:
            worksheet_number (int): Nummer des zu ladenden Arbeitsblatts (0-basiert)
        """
        wks = self.sh[worksheet_number]
        self.pharmacists_label = wks.title
        self.df = wks.get_as_df()
        self.modified_df = pd.DataFrame(columns=COLUMNS)

    def is_not_empty(self, cell):
        """
        Prüft, ob eine Zelle nicht leer ist.

        Args:
            cell: Zellenwert

        Returns:
            bool: True wenn nicht leer, sonst False
        """
        if cell == "":
            return False
        else:
            return True

    def process_row(self):
        """
        Verarbeitet alle Zeilen des Input DataFrames.

        Konvertiert jede Zeile der Adjazenzliste in Triple-Strukturen.
        """
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
        """
        Schreibt den Output DataFrame in ein Google Sheet.

        Args:
            number (int): Nummer des Arbeitsblatts im Output-Sheet
        """
        spreadsheet = self.gc.open(OUTPUT_SHEET)
        worksheet = spreadsheet[number]
        worksheet.clear()
        worksheet.title = self.pharmacists_label
        self.add_nodes_to_df()
        # Write the DataFrame to the Google Sheet
        worksheet.set_dataframe(self.modified_df, (1, 1))
        print("successfully wrote to google sheet")

    def add_nodes_to_df(self):
        """
        Sammelt alle Source- und Target-Nodes zum Nodes-DataFrame.
        """
        self.intermediate_df = self.modified_df.loc[:,"Source_Node"]
        self.nodes_df = pd.concat([self.nodes_df, self.intermediate_df], ignore_index=True)
        self.intermediate_df = self.modified_df.loc[:,"Target_Node"]
        self.nodes_df = pd.concat([self.nodes_df, self.intermediate_df], ignore_index=True)


    def write_nodes_sheet(self):
        """
        Schreibt alle gesammelten Nodes in das Nodes-Arbeitsblatt.

        Entfernt Duplikate aus den Nodes vor dem Schreiben.
        """
        spreadsheet = self.gc.open(OUTPUT_SHEET)
        worksheet = spreadsheet.worksheet_by_title("Nodes")
        self.nodes_df = self.nodes_df.drop_duplicates(keep="first")
        worksheet.set_dataframe(self.nodes_df, (1, 1))
        print("successfully wrote Nodes to google sheet")



def main():
    """
    Hauptfunktion zur Verarbeitung aller Pharmazeuten-Daten.

    Verarbeitet die Arbeitsblätter ab FIRST_SOURCE_WKS und exportiert die Ergebnisse.

    Returns:
        int: Exit-Code (0 bei Erfolg)
    """
    count = 0
    maker = SheetMaker(SOURCE_SHEET)
    for worksheet_number in range(FIRST_SOURCE_WKS, 8):
        maker.get_df(worksheet_number)
        maker.process_row()
        maker.write_xlsx(count)
        print(f"Processed {count+1}.Sheet")
        count += 1
    maker.write_nodes_sheet()
    return 0


sys.exit(main())
