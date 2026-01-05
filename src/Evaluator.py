"""
Evaluator.py

Dieses Modul evaluiert und vergleicht Medication Review Triples von verschiedenen Pharmazeuten.

Hauptfunktionen:
- Zugriff auf Google Sheets und Laden von Daten
- Vergleich von Outcomes zwischen verschiedenen Pharmazeuten
- Sammeln und Organisieren von Triples (Source_Node, Relationship, Target_Node)
- Export der Evaluierungsergebnisse nach Google Sheets

Datenverarbeitung:
1. Laden der Daten aus dem "Total" Arbeitsblatt
2. Filterung nach bestimmten Relationship-Typen (hasOutcome, needs, needsClarification, needsRequestOf)
3. Gruppierung identischer Triples und Zählung der Übereinstimmung zwischen Pharmazeuten
4. Export in das "Outcome_Comparism" Arbeitsblatt

TripleTree:
    Enthält alle Triples und ermöglicht Zugriff auf Triples nach source, edge, target.
    Value ist das Triple selbst.

class Triple:
    source: Optional[str] - Quell-Node
    edge: Optional[str] - Relationship/Kante
    target: Optional[str] - Ziel-Node
    pharmacist_1: [int] - Liste an Row_Numbers für Pharmazeut 1

Autor: Interpolar Project
"""
import dataclasses
import pygsheets
import pandas as pd
import sys


# Konstanten
SERVICE_FILE = "creds.json"  # Pfad zur Google Service Account Datei
COLUMNS = ("Source_Node", "Relationship", "Target_Node", "Pharmacist_1", "Pharmacist_2", "Pharmacist_3", "Pharmacist_4", "Pharmacist_5", "Count")  # Spalten im Output
SOURCE_SHEET = "Medication_Review_Triples"  # Name des Quell-Google Sheets
SOURCE_WORKSHEET = "Total"  # Name des Quell-Arbeitsblatts
OUTPUT_SHEET = "Evaluation"  # Name des Ziel-Google Sheets
OUTPUT_WORKSHEET = "Outcome_Comparism"  # Name des Ziel-Arbeitsblatts
# Spalten im Quell-Sheet
PHARMACIST = "Pharmacists_Label"  # Spalte mit Pharmazeuten-Label
ROW = "Subprocess"  # Spalte mit Subprocess-Nummer

class Evaluator:
    """
    Hauptklasse zur Evaluation und zum Vergleich von Medication Review Outcomes.

    Diese Klasse lädt Daten aus Google Sheets, vergleicht Triples zwischen verschiedenen
    Pharmazeuten und exportiert die Ergebnisse.

    Attributes:
        source_data (pd.DataFrame): Geladene Quelldaten aus Google Sheets
        output_data (pd.DataFrame): Aufbereitete Ausgabedaten
        outcome_organizer (dict): Dictionary zur Organisation von Triples mit ihren Vorkommen
    """
    @dataclasses.dataclass(frozen=True)
    class Triple:
        """
        Dataclass zur Repräsentation eines RDF-Triples.

        Attributes:
            source (str): Quell-Node des Triples
            edge (str): Relationship/Kante zwischen Source und Target
            target (str): Ziel-Node des Triples
        """
        source: str
        edge: str
        target: str

    def __init__(self):
        """
        Initialisiert den Evaluator und lädt die Quelldaten.

        Lädt automatisch die Daten aus dem SOURCE_SHEET/SOURCE_WORKSHEET und
        initialisiert leere Output-Strukturen.
        """
        self.source_data = self.load_data(SOURCE_SHEET, SOURCE_WORKSHEET)
        self.output_data = pd.DataFrame(columns=COLUMNS)
        self.outcome_organizer = {}

    def load_data(self, google_sheet, worksheet_title):
        """
        Lädt Daten aus einem Google Sheet Arbeitsblatt.

        Args:
            google_sheet (str): Name des Google Sheets
            worksheet_title (str): Titel des Arbeitsblatts

        Returns:
            pd.DataFrame: Geladene Daten als DataFrame
        """
        gc = pygsheets.authorize(service_file=SERVICE_FILE)
        sh = gc.open(google_sheet)
        ws = sh.worksheet_by_title(worksheet_title)
        data = ws.get_as_df()
        return data

    def clean_data(self):
        """
        Bereinigt die Quelldaten durch Filterung nach Outcome-Relationships.

        Entfernt alle Zeilen, die keine Outcome-Relationships enthalten
        (z.B. needs, needsRequestOf, needsClarificationOf, hasOutcome, etc.).
        """
        for index, row in self.source_data.iterrows():
            if not self.is_an_outcome(row["Relationship"]):
                self.source_data.drop(index, inplace=True)

    def write_organizer(self):
        """
        Organisiert die Triples nach ihrem Vorkommen bei verschiedenen Pharmazeuten.

        Erstellt einen Dictionary (outcome_organizer) mit Triples als Keys und Listen
        von [Pharmazeut, Row-Nummer] Paaren als Values.
        """
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
        """
        Erstellt die Output-Daten aus dem outcome_organizer.

        Konvertiert die organisierten Triples in einen DataFrame mit Spalten für jeden
        Pharmazeuten und zählt die Anzahl der Pharmazeuten, die das Triple identifiziert haben.
        """
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
        """
        Prüft, ob eine Relationship ein Outcome darstellt.

        Args:
            edge (str): Die zu prüfende Relationship

        Returns:
            bool: True wenn die Relationship ein Outcome ist, sonst False
        """
        if edge in ["needs", "needsRequestOf", "needsClarificationOf", "hasOutcome", "giveProposalOf", "needsResearchIn"]:
            return True
        else:
            return False

    def process_data(self):
        """
        Führt die vollständige Datenverarbeitung durch.

        Verarbeitet die Quelldaten durch Bereinigung, Organisation und Export
        der Evaluierungsergebnisse nach Google Sheets.
        """
        self.clean_data()
        self.write_organizer()
        self.write_output_data()
        self.write_to_google_sheet()

    def write_to_google_sheet(self):
        """
        Schreibt die Output-Daten in ein Google Sheet.

        Löscht das bestehende Arbeitsblatt und schreibt die neuen Evaluierungsdaten.
        """
        gc = pygsheets.authorize(service_file=SERVICE_FILE)
        sh = gc.open(OUTPUT_SHEET)
        ws = sh.worksheet_by_title(OUTPUT_WORKSHEET)
        ws.clear()
        ws.set_dataframe(self.output_data, (1, 1))
        print("successfully wrote to google sheet")

def main():
    """
    Hauptfunktion zur Ausführung der Evaluation.

    Erstellt einen Evaluator und führt die Datenverarbeitung durch.

    Returns:
        int: Exit-Code (0 bei Erfolg)
    """
    evaluator = Evaluator()
    evaluator.process_data()

    return 0

sys.exit(main())
