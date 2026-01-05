"""
SheetMaker_StartEnd.py

Dieses Modul konvertiert Adjazenzlisten aus Google Sheets in Triple-Strukturen mit Start- und End-Nodes.

Hauptfunktionen:
- Laden von Adjazenzlisten aus Google Sheets
- Konvertierung in Triple-Strukturen (Source_Node, Relationship, Target_Node)
- Automatisches Hinzufügen von Start- und End-Nodes zu jedem Prozess
- Sammlung aller eindeutigen Nodes
- Export nach Google Sheets

Besonderheit:
Jeder Prozess beginnt mit "start" -> "is" -> [erster Node] und endet mit
[letzter Node] -> "is" -> "end", um klare Anfangs- und Endpunkte zu definieren.

"""

import pygsheets
import sys
import pandas as pd

# Konstanten
SERVICE_FILE = "creds.json"  # Pfad zur Google Service Account Datei
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Subprocess", "Step", "Pharmacists_Label")  # Spalten im Output
SOURCE_SHEET = "Adjazenzlisten_Medication_Review"  # Quell-Google Sheet
OUTPUT_SHEET = "Medication_Review_Triples"  # Ziel-Google Sheet
FIRST_SOURCE_WKS = 3  # Erstes zu verarbeitendes Arbeitsblatt


class SheetMaker(object):
    """
    Klasse zur Konvertierung von Adjazenzlisten in Triples mit Start/End-Nodes.

    Diese Klasse lädt Adjazenzlisten, fügt Start- und End-Nodes hinzu und
    exportiert die Triples zurück nach Google Sheets.

    Attributes:
        worksheet_number (int): Nummer des aktuellen Arbeitsblatts
        gc: Google Sheets Client-Objekt
        input_sheet: Quell-Spreadsheet-Objekt
        output_sheet: Ziel-Spreadsheet-Objekt
        df_in (pd.DataFrame): Input DataFrame mit Adjazenzliste
        df_out (pd.DataFrame): Output DataFrame mit Triples
        df_nodes (pd.DataFrame): DataFrame mit gesammelten Nodes
        df_total (pd.DataFrame): Gesamt-DataFrame aller Pharmazeuten
        triple_number (int): Zähler für Triple-Nummern
        pharmacists_label (str): Label des aktuellen Pharmazeuten
    """
    def __init__(self):
        """
        Initialisiert den SheetMaker und stellt Verbindungen zu Google Sheets her.
        """
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
        """
        Lädt Daten aus einem Arbeitsblatt in einen DataFrame.

        Args:
            worksheet_number (int): Nummer des zu ladenden Arbeitsblatts (0-basiert)
        """
        self.df_out = pd.DataFrame(columns=COLUMNS)
        wks = self.input_sheet[worksheet_number]
        self.pharmacists_label = wks.title
        self.df_in = wks.get_as_df()

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

    def add_end(self, source, row_number):
        """
        Fügt einen End-Node für einen Prozess hinzu.

        Args:
            source (str): Quell-Node, der auf "end" verweisen soll
            row_number (int): Nummer der Zeile/des Prozesses
        """
        target = "end"
        edge = "is"
        data = [source, edge, target, row_number, self.triple_number+2, self.pharmacists_label]
        new_row = pd.DataFrame([data], columns=self.df_out.columns)
        self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)

    def add_start(self, target, row_number):
        """
        Fügt einen Start-Node für einen Prozess hinzu.

        Erstellt Triples von "start" zu allen Ziel-Nodes (bei mehreren durch ";" getrennt).

        Args:
            target (str): Ziel-Node(s), zu denen "start" verweisen soll
            row_number (int): Nummer der Zeile/des Prozesses
        """
        source = "start"
        edge = "is"
        target_list = target.split(";")
        for target in target_list:
            self.triple_number += 1
            data = [source, edge, target, row_number, self.triple_number, self.pharmacists_label]
            new_row = pd.DataFrame([data], columns=self.df_out.columns)
            self.df_out = pd.concat([self.df_out, new_row], ignore_index=True)

    def process_row(self):
        """
        Verarbeitet alle Zeilen des Input DataFrames.

        Fügt Start-Nodes, alle Triples und End-Nodes für jeden Prozess hinzu.
        """
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
        """
        Schreibt den Output DataFrame in ein Google Sheet.

        Erstellt ein neues Arbeitsblatt falls es nicht existiert und schreibt die Daten.
        """
        try:
            worksheet = self.output_sheet.worksheet_by_title(self.pharmacists_label)
        except pygsheets.exceptions.WorksheetNotFound:
            worksheet = self.output_sheet.add_worksheet(self.pharmacists_label)
        worksheet.clear()
        self.add_nodes_to_df()
        # Write the DataFrame to the Google Sheet
        worksheet.set_dataframe(self.df_out, (1, 1))
        print("successfully wrote to google sheet")

    def add_nodes_to_df(self):
        """
        Sammelt alle Source- und Target-Nodes zum Nodes-DataFrame.

        Fügt auch den aktuellen Output zum Gesamt-DataFrame hinzu.
        """
        for node_type in ["Source_Node", "Target_Node"]:
            self.df_nodes = pd.concat([self.df_nodes, self.df_out.loc[:, node_type]], ignore_index=True)
        self.df_total = pd.concat([self.df_total, self.df_out], ignore_index=True)

    def write_nodes_sheet(self):
        """
        Schreibt alle gesammelten Nodes und Total-Daten in separate Arbeitsblätter.

        Entfernt Duplikate aus den Nodes und schreibt sowohl das Nodes- als auch
        das Total-Arbeitsblatt.
        """
        worksheet = self.output_sheet.worksheet_by_title("Nodes")
        worksheet.clear()
        self.df_nodes = self.df_nodes.drop_duplicates(keep="first")
        worksheet.set_dataframe(self.df_nodes, (1, 1))
        print("successfully wrote Nodes to google sheet")
        worksheet = self.output_sheet.worksheet_by_title("Total")
        worksheet.clear()
        worksheet.set_dataframe(self.df_total, (1, 1))
        print("successfully wrote Total google sheet")



def main():
    """
    Hauptfunktion zur Verarbeitung aller Pharmazeuten-Daten.

    Verarbeitet die Arbeitsblätter ab FIRST_SOURCE_WKS und exportiert die Ergebnisse.

    Returns:
        int: Exit-Code (0 bei Erfolg)
    """
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
