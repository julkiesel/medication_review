import pygsheets
import sys
from rdflib import Graph, URIRef


class RDFMaker(object):
    def __init__(self, google_sheet, worksheet_number):
        self.worksheet_number = worksheet_number
        # authorization
        self.gc = pygsheets.authorize(service_file="../creds.json")
        # open the Google spreadsheet
        self.sh = self.gc.open(google_sheet)
        # select the first sheet
        self.wks = self.sh[self.worksheet_number]
        self.df = self.wks.get_as_df()
        self.graph = Graph()

    def is_not_empty(self, cell):
        if cell == "":
            return False
        else:
            return True

    def add_node(self, subject_raw, predicate, obj_raw):
        subject_list = subject_raw.split(";")
        obj_list = obj_raw.split(";")
        for subject in subject_list:
            for obj in obj_list:
                self.graph.add((self.create_uri(subject), self.create_uri(predicate), self.create_uri(obj)))

    def create_uri(self, uri):
        return URIRef('https://interpolar.com/'+str(uri))

    def build_graph(self):
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
        # Serialize the RDF graph to a file
        output_file = f"RDF/session_{self.worksheet_number-1}.rdf"
        self.graph.serialize(destination=output_file, format='xml')

    def sanitize_string(self, uri):
        # Replace spaces with underscores and remove invalid characters
        return str(uri).replace(" ", "").replace(",", "").replace(":", "")

    def print(self):
        print(self.df)


def main():
    google_sheet = "Graph des Medication Review"
    for (worksheet_number) in range(3, 8):
        maker = RDFMaker(google_sheet, worksheet_number)
        maker.build_graph()
        maker.write_rdf()
    return 0

sys.exit(main())
