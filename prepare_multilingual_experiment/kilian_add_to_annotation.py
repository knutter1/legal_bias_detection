import json
from pymongo import MongoClient
import os

def connect_to_mongo():
    """Stellt eine Verbindung zur MongoDB her und gibt die Collection zurück."""
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        db = client["court_decisions"]
        collection = db["judgments"]
        print("✅ Erfolgreich mit MongoDB verbunden.")
        return collection
    except Exception as e:
        print(f"❌ Fehler bei der Verbindung zu MongoDB: {e}")
        raise

def add_selection_flag_for_kilian():
    """
    Fügt allen Dokumenten, die eine Annotation von 'Kilian Lüders' enthalten,
    das Attribut 'selected_for_annotation: true' hinzu.
    """
    collection = connect_to_mongo()
    if collection is None:
        return

    print("\nSuche nach Annotationen von 'Kilian Lüders', um sie zu markieren...")

    # 1. Definiere die Abfrage, um die relevanten Dokumente zu finden.
    #    Die Abfrage durchsucht das verschachtelte Array nach dem Annotator.
    query = {
        "ollama_responses.response.biases.annotations.annotator": "Kilian Lüders"
    }

    # 2. Definiere das Update, das das neue Feld auf oberster Ebene setzt.
    update = {
        "$set": {
            "selected_for_annotation": True
        }
    }

    # 3. Führe das Update für alle passenden Dokumente aus.
    try:
        result = collection.update_many(query, update)
        print(f"✅ Erfolgreich! {result.modified_count} Dokument(e) wurden markiert.")
    except Exception as e:
        print(f"❌ Fehler beim Aktualisieren der Dokumente: {e}")


def test_and_save_first_element():
    """
    Holt das erste Dokument mit dem neuen Attribut aus der Datenbank
    und speichert es zur Überprüfung als JSON-Datei.
    """
    collection = connect_to_mongo()
    if collection is None:
        return

    print("\nSuche nach einem markierten Dokument für den Test...")

    # Finde ein beliebiges Dokument, das das neue Flag hat.
    test_element = collection.find_one({"selected_for_annotation": True})

    if test_element:
        print("✅ Ein markiertes Element wurde gefunden. Speichere es in einer JSON-Datei...")
        file_path = os.path.join(os.path.dirname(__file__), "test_element_selected_for_annotation.json")

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                # json.dump zur Speicherung des Dokuments
                # default=str wird verwendet, um MongoDB-spezifische Typen (wie ObjectId) in Strings umzuwandeln
                json.dump(test_element, f, ensure_ascii=False, indent=4, default=str)
            print(f"✅ Test-Datei erfolgreich gespeichert unter: {file_path}")
        except Exception as e:
            print(f"❌ Fehler beim Speichern der JSON-Datei: {e}")
    else:
        print("⚠️ Kein Dokument mit dem Attribut 'selected_for_annotation: true' gefunden.")


if __name__ == "__main__":
    # Schritt 1: Die Markierung hinzufügen
    add_selection_flag_for_kilian()

    # Schritt 2: Die Testfunktion ausführen, um das Ergebnis zu überprüfen
    test_and_save_first_element()