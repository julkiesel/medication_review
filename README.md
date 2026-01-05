# Interpolar - Medication Review Analysis

Ein Python-basiertes Projekt zur Analyse und Visualisierung von Medication Review Prozessen durch verschiedene Pharmazeuten.

## Übersicht

Dieses Projekt ermöglicht die Konvertierung, Analyse und Visualisierung von Medication Review Daten. 
Es transformiert Adjazenzlisten aus Google Sheets in strukturierte Triple-Formate (RDF, Neo4j) 
und bietet umfassende Evaluierungsmöglichkeiten.

## Hauptfunktionen

### 1. Datenkonvertierung
- **GraphMaker.py**: Konvertiert Adjazenzlisten in Triple-Strukturen mit EPA-Filterung
- **SheetMaker.py**: Einfache Konvertierung von Adjazenzlisten zu Triples
- **SheetMaker_StartEnd.py**: Konvertierung mit automatischen Start/End-Nodes
- **RDFMaker.py**: Export als RDF/XML für Semantic Web Anwendungen
- **Neo4jUploader.py**: Export als CSV für Neo4j Import

### 2. Evaluation & Analyse
- **Evaluator.py**: Vergleicht Medication Review Outcomes zwischen Pharmazeuten
- **Evaluationscript.ipynb**: Jupyter Notebook für umfassende Neo4j-basierte Analysen

## Installation

### Voraussetzungen
- Python 3.7+
- Google Service Account mit Zugriff auf Google Sheets (und damit das Adjazenzlisten Dokument)
- Neo4j Datenbank (für Evaluationscript)

### Dependencies installieren

```bash
pip install -r requirements.txt
```

### Google Sheets Authentifizierung

1. Erstellen Sie einen Google Service Account
2. Laden Sie die Credentials als `creds.json` herunter
3. Platzieren Sie `creds.json` im Hauptverzeichnis
4. Geben Sie dem Service Account Zugriff auf die relevanten Google Sheets

## Verwendung

### 1. Adjazenzlisten zu Triples konvertieren

```bash
# Mit EPA-Filterung
python src/GraphMaker.py

# Einfache Konvertierung
python src/SheetMaker.py

# Mit Start/End-Nodes
python src/SheetMaker_StartEnd.py
```

### 2. RDF-Export erstellen

```bash
python src/RDFMaker.py
```

### 3. CSV für Neo4j erstellen

```bash
python src/Neo4jUploader.py
```

### 4. Pharmazeuten-Vergleich durchführen

```bash
python src/Evaluator.py
```

### 5. Neo4j Analyse (Jupyter Notebook)

```bash
jupyter notebook src/Evaluationscript.ipynb
```

## Autorin

Julia Kiesel


