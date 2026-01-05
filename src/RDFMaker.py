"""
RDFMaker.py

Dieses Modul konvertiert Daten aus Google Sheets in RDF (Resource Description Framework) Format.

Hauptfunktionen:
- Laden von Medication Review Daten aus Google Sheets
- Konvertierung von Adjazenzlisten in RDF-Triples
- Serialisierung der RDF-Graphen als XML-Dateien
- Bereinigung und Normalisierung von Strings für URIs

Das Modul verwendet die rdflib Bibliothek zur Erstellung von RDF-Graphen und
speichert diese als XML-Dateien für weitere Verarbeitung.

"""

import pygsheets
import sys
from rdflib import Graph, URIRef


class RDFMaker(object):
    """
    Klasse zur Erstellung von RDF-Graphen aus Google Sheets Daten.

    Diese Klasse lädt Daten aus einem Google Sheet, konvertiert sie in
    RDF-Triples und serialisiert diese als RDF/XML.

    Attributes:
        worksheet_number (int): Nummer des zu verarbeitenden Arbeitsblatts
        gc: Google Sheets Client-Objekt
        sh: Google Sheets Spreadsheet-Objekt
        wks: Worksheet-Objekt
        df (pd.DataFrame): DataFrame mit geladenen Daten
        graph (Graph): rdflib Graph-Objekt für RDF-Triples
    """
    def __init__(self, google_sheet, worksheet_number):
        """
        Initialisiert den RDFMaker mit einem Google Sheet und Arbeitsblatt.

        Args:
            google_sheet (str): Name des Google Sheets
            worksheet_number (int): Nummer des Arbeitsblatts (0-basiert)
        """
        self.worksheet_number = worksheet_number
        # authorization
        self.gc = pygsheets.authorize(service_file="creds.json")
        # open the Google spreadsheet
        self.sh = self.gc.open(google_sheet)
        # select the first sheet
        self.wks = self.sh[self.worksheet_number]
        self.df = self.wks.get_as_df()
        self.graph = Graph()

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

    def add_node(self, subject_raw, predicate, obj_raw):
        """
        Fügt Triples zum RDF-Graphen hinzu.

        Splittet Subject und Object bei mehreren Werten (getrennt durch ";")
        und erstellt für jede Kombination ein RDF-Triple.

        Args:
            subject_raw (str): Subject(s) des Triples, möglicherweise mehrere durch ";" getrennt
            predicate (str): Prädikat/Relationship des Triples
            obj_raw (str): Object(s) des Triples, möglicherweise mehrere durch ";" getrennt
        """
        subject_list = subject_raw.split(";")
        obj_list = obj_raw.split(";")
        for subject in subject_list:
            for obj in obj_list:
                self.graph.add((self.create_uri(subject), self.create_uri(predicate), self.create_uri(obj)))

    def create_uri(self, uri):
        """
        Erstellt eine URI für einen String.

        Args:
            uri (str): String, der in eine URI konvertiert werden soll

        Returns:
            URIRef: RDFlib URIRef-Objekt
        """
        return URIRef('https://interpolar.com/'+str(uri))

    def build_graph(self):
        """
        Erstellt den RDF-Graphen aus dem DataFrame.

        Iteriert durch den DataFrame und fügt alle Triples zum RDF-Graphen hinzu.
        """
        # Iterate through the dataframe to add triples to the RDF graph
        for index, row in self.df.iterrows():
            for col in range(1, len(row), 2):  # Iterate over pairs of columns (Edge, Target)
                source = row.iloc[col-1]
                edge = row.iloc[col]
                target = row.iloc[col + 1]
                if self.is_not_empty(edge) and self.is_not_empty(target):
                    subject = self.sanitize_string(source)
                    predicate = self.sanitize_string(edge)
                    obj = self.sanitize_string(target)
                    self.add_node(subject, predicate, obj)
                else:
                    break
    def write_rdf(self):
        """
        Serialisiert den RDF-Graphen als XML-Datei.

        Speichert den RDF-Graphen im RDF/XML Format in das RDF Verzeichnis.
        """
        # Serialize the RDF graph to a file
        output_file = f"RDF/session_{self.worksheet_number-1}.rdf"
        self.graph.serialize(destination=output_file, format='xml')

    def sanitize_string(self, uri):
        """
        Bereinigt einen String für die Verwendung in URIs.

        Entfernt Leerzeichen, Kommas und Doppelpunkte.

        Args:
            uri (str): Zu bereinigender String

        Returns:
            str: Bereinigter String
        """
        # Replace spaces with underscores and remove invalid characters
        return str(uri).replace(" ", "").replace(",", "").replace(":", "")

    def print(self):
        """
        Gibt den DataFrame auf der Konsole aus.

        Hilfsfunktion für Debugging-Zwecke.
        """
        print(self.df)


def main():
    """
    Hauptfunktion zur Erstellung von RDF-Dateien aus Google Sheets.

    Verarbeitet mehrere Arbeitsblätter und erstellt für jedes eine separate RDF-Datei.

    Returns:
        int: Exit-Code (0 bei Erfolg)
    """
    google_sheet = "Graph des Medication Review"
    for (worksheet_number) in range(3, 8):
        maker = RDFMaker(google_sheet, worksheet_number)
        maker.build_graph()
        maker.write_rdf()
    return 0

sys.exit(main())
