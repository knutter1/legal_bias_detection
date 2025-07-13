import json
from pymongo import MongoClient

class AmericanJudgmentInserter:
    """
    Diese Klasse fügt vietnamesische Urteile aus einer JSON-Datei in eine MongoDB-Datenbank ein.
    """

    def __init__(self, mongo_uri='mongodb://localhost:27017/', db_name='court_decisions'):
        """
        Initialisiert den Inserter und stellt eine Verbindung zur MongoDB her.
        """
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db['judgments']

    def get_next_id(self):
        """
        Ermittelt die nächsthöhere verfügbare ID in der Sammlung.
        """
        max_id_doc = self.collection.find_one(sort=[("id", -1)])
        return (max_id_doc['id'] + 1) if max_id_doc else 1

    def read_prompt_file(self, filename):
        """
        Liest den Inhalt einer Prompt-Datei.
        """
        with open(filename, 'r', encoding='utf-8') as f:
            return f.read().strip()

    def insert_judgments(self, json_file, prompt_summary_file, prompt_bias_file):
        """
        Liest die Urteile aus der JSON-Datei und fügt sie in die Datenbank ein.
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            judgments = json.load(f)

        prompt_summary = self.read_prompt_file(prompt_summary_file)
        prompt_bias_detection = self.read_prompt_file(prompt_bias_file)

        next_id = self.get_next_id()

        for i, judgment_data in enumerate(judgments):
            # Erstelle eine Kopie, um alle originalen Attribute zu übernehmen
            new_judgment = judgment_data.copy()

            # Füge neue Felder hinzu oder überschreibe bestehende
            new_judgment["id"] = next_id + i
            new_judgment["selected_for_annotation"] = True
            new_judgment["prompt_summary"] = prompt_summary
            new_judgment["prompt_bias_detection"] = prompt_bias_detection

            # Benenne 'origin_text' in 'full_text' um und entferne den alten Schlüssel
            if "origin_text" in new_judgment:
                new_judgment["full_text"] = new_judgment.pop("origin_text")

            # Die Felder 'origin_url', 'language' und alle anderen sind bereits in der Kopie enthalten

            self.collection.insert_one(new_judgment)
            print(f"Urteil mit ID {new_judgment['id']} eingefügt.")

    def test_first_vietnamese_element(self):
        """
        Holt das erste englische Element aus der Datenbank und speichert es in einer JSON-Datei.
        """
        first_english_judgment = self.collection.find_one({"language": "English"})
        if first_english_judgment:
            # Konvertieren Sie die ObjectId in einen String, um sie JSON-serialisierbar zu machen
            if '_id' in first_english_judgment:
                first_english_judgment['_id'] = str(first_english_judgment['_id'])

            with open('test_english_element.json', 'w', encoding='utf-8') as f:
                json.dump(first_english_judgment, f, ensure_ascii=False, indent=4)
            print("Erstes englisches Urteil in 'test_english_element.json' gespeichert.")
        else:
            print("Kein englisches Urteil in der Datenbank gefunden.")


if __name__ == '__main__':
    # Annahmen über die Dateipfade
    # Diese Dateien müssen im selben Verzeichnis wie das Skript liegen
    english_json_file = 'supreme_2005_2025.json'
    prompt_summary_file = 'prompt_summary_english.txt'
    prompt_bias_file = 'prompt_bias_detection_english.txt'

    inserter = AmericanJudgmentInserter()
    inserter.insert_judgments(english_json_file, prompt_summary_file, prompt_bias_file)
    inserter.test_first_vietnamese_element()