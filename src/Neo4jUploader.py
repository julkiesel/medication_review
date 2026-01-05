"""
Neo4jUploader.py

Dieses Modul lädt Daten aus Google Sheets herunter und exportiert sie als CSV-Dateien.
Die CSV-Dateien können dann in Neo4j importiert werden.

Hauptfunktionen:
- Verbindung zu Google Sheets über Service Account
- Extraktion von Daten aus mehreren Arbeitsblättern
- Export als CSV-Dateien für Neo4j-Import

Autor: Interpolar Project
"""

import pygsheets
import sys
import pandas as pd
from os import path

# Konstanten
SERVICE_FILE = "creds.json"  # Pfad zur Google Service Account Datei
SOURCE_SHEET = "Medication_Review_Triples"  # Name des Google Sheets
DIR_OUT = path.abspath("CSV")  # Ausgabeverzeichnis für CSV-Dateien


class SheetMaker(object):
    """
    Klasse zum Laden von Google Sheets und Export als CSV-Dateien.

    Attributes:
        worksheet_number (int): Nummer des aktuellen Arbeitsblatts
        gc: Google Sheets Client-Objekt
        sh: Google Sheets Spreadsheet-Objekt
        df (pd.DataFrame): DataFrame mit den geladenen Daten
        pharmacists_label (str): Titel des aktuellen Arbeitsblatts
    """
    def __init__(self, google_sheet):
        """
        Initialisiert den SheetMaker mit einem Google Sheet.

        Args:
            google_sheet (str): Name des Google Sheets

        Raises:
            Exception: Bei Verbindungsproblemen mit Google Sheets
        """
        self.worksheet_number = 0
        self.gc = pygsheets.authorize(service_file=SERVICE_FILE)
        self.sh = self.gc.open(google_sheet)
        self.df = pd.DataFrame()
        self.pharmacists_label = "not set"

    def get_df(self, worksheet_number):
        """
        Lädt Daten aus einem bestimmten Arbeitsblatt in einen DataFrame.

        Args:
            worksheet_number (int): Nummer des zu ladenden Arbeitsblatts (0-basiert)
        """
        wks = self.sh[worksheet_number]
        self.pharmacists_label = wks.title
        self.df = wks.get_as_df()

    def write_csv(self):
        """
        Schreibt den aktuellen DataFrame als CSV-Datei.

        Die Datei wird im DIR_OUT Verzeichnis gespeichert mit dem Namen des Arbeitsblatts.
        """
        self.df.to_csv(path.join(DIR_OUT, self.pharmacists_label + ".csv"), quotechar='"', index=False)

def main():
    """
    Hauptfunktion zum Verarbeiten von Google Sheets zu CSV-Dateien.

    Verarbeitet die ersten 5 Arbeitsblätter des SOURCE_SHEET und exportiert sie als CSV.

    Returns:
        int: Exit-Code (0 bei Erfolg)
    """
    maker = SheetMaker(SOURCE_SHEET)
    for worksheet_number in range(0, 5):
        maker.get_df(worksheet_number)
        maker.write_csv()
        print(f"Processed {worksheet_number+1}.Sheet")

    return 0


sys.exit(main())