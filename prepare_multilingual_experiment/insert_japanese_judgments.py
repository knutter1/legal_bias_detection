import json
import os
from tqdm import tqdm
from bson import ObjectId

# Importieren Sie die bestehende Funktion, um die Verbindung zur DB herzustellen
from prepare_data import connect_to_mongo


class JapaneseCaseImporter:
    """
    Diese Klasse importiert japanische Urteile aus einer JSONL-Datei in die MongoDB.
    Sie stellt sicher, dass keine Duplikate erstellt werden, weist neue, fortlaufende IDs zu
    und fügt Prompts aus externen Dateien hinzu.
    """

    def __init__(self):
        """Initialisiert den Importer, verbindet sich mit MongoDB und holt die letzte ID."""
        self.collection = connect_to_mongo()
        self.last_id = self._get_last_id()
        print(f"Verbindung zur MongoDB hergestellt. Die nächste verfügbare ID ist {self.last_id + 1}.")

    def _get_last_id(self):
        """Holt die höchste bestehende 'id' aus der Collection."""
        last_document = self.collection.find_one(sort=[("id", -1)])
        if last_document and 'id' in last_document:
            return last_document['id']
        return 0  # Falls die Collection leer ist oder keine numerischen IDs hat

    def _read_prompt_file(self, filepath):
        """Liest den Inhalt einer Textdatei."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Fehler: Die Prompt-Datei '{filepath}' wurde nicht gefunden.")
            return ""

    def process_jsonl_file(self, jsonl_filepath, summary_prompt_path, bias_prompt_path):
        """
        Verarbeitet die JSONL-Datei und fügt die Urteile in die Datenbank ein.
        Verwendet upsert, um Duplikate basierend auf der 'origin_url' zu vermeiden.
        """
        summary_prompt = self._read_prompt_file(summary_prompt_path)
        bias_prompt = self._read_prompt_file(bias_prompt_path)

        if not summary_prompt or not bias_prompt:
            print("Import wird wegen fehlender Prompt-Dateien abgebrochen.")
            return

        print(f"Verarbeite die Datei: {jsonl_filepath}")

        try:
            with open(jsonl_filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in tqdm(lines, desc="Importiere japanische Urteile"):
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        print(f"Warnung: Ungültige JSON-Zeile übersprungen: {line.strip()}")
                        continue

                    # Erstelle das neue Dokument für die Datenbank
                    new_document = {}

                    # Meta-Daten kopieren, die nicht explizit gemappt werden
                    for key, value in data.items():
                        if key not in ["origin_text", "origin_url", "language"]:
                            new_document[key] = value

                    # Explizite Feldzuweisung
                    new_document['full_text'] = data.get("origin_text", "")
                    new_document['origin_url'] = data.get("origin_url", "")
                    new_document['language'] = data.get("language", "japanese")  # Fallback

                    # Prompts hinzufügen
                    new_document['prompt_summary'] = summary_prompt
                    new_document['prompt_bias_detection'] = bias_prompt

                    # Prüfen, ob das Dokument bereits existiert (basierend auf URL)
                    existing_doc = self.collection.find_one({"origin_url": new_document['origin_url']})

                    if existing_doc:
                        # Dokument aktualisieren, aber ID behalten
                        self.collection.update_one(
                            {"origin_url": new_document['origin_url']},
                            {"$set": new_document}
                        )
                    else:
                        # Neue ID zuweisen und Dokument einfügen
                        self.last_id += 1
                        new_document['id'] = self.last_id
                        self.collection.insert_one(new_document)

        except FileNotFoundError:
            print(f"Fehler: Die Eingabedatei '{jsonl_filepath}' wurde nicht gefunden.")
        except Exception as e:
            print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")

    def test_first_japanese_element(self, output_filename="test_japanese_element.json"):
        """
        Holt das erste japanische Urteil aus der DB und speichert es in einer JSON-Datei.
        """
        print(f"Suche erstes japanisches Element zum Testen...")
        element = self.collection.find_one({"language": "Japanese"})

        if not element:
            print("Kein japanisches Element in der Datenbank gefunden.")
            return

        # Konvertiere ObjectId in einen String für die JSON-Serialisierung
        if '_id' in element and isinstance(element['_id'], ObjectId):
            element['_id'] = str(element['_id'])

        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(element, f, ensure_ascii=False, indent=4)
            print(f"Test erfolgreich. Das erste japanische Element wurde in '{output_filename}' gespeichert.")
        except Exception as e:
            print(f"Fehler beim Schreiben der Testdatei: {e}")


if __name__ == "__main__":
    # Pfade zu den Eingabedateien
    # Annahme: Alle Dateien befinden sich im selben Verzeichnis wie das Skript
    jsonl_file = "japanese_legal_cases.jsonl"
    summary_prompt_file = "prompt_summary_japanese.txt"
    bias_prompt_file = "prompt_bias_detection_japanese.txt"

    # Initialisiere und starte den Import-Prozess
    importer = JapaneseCaseImporter()
    importer.process_jsonl_file(jsonl_file, summary_prompt_file, bias_prompt_file)

    # Führe die Testfunktion aus
    importer.test_first_japanese_element()