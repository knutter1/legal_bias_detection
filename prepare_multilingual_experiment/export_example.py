import os
import json
from bson import ObjectId

# Wiederverwendung der Verbindungsfunktion aus Ihrem bestehenden Skript
from prepare_data import connect_to_mongo


def get_nth_judgment(n: int, language):
    """
    Holt das n-te vietnamesische Urteil aus der MongoDB und speichert es
    als JSON-Datei im Unterordner 'examples'.

    Args:
        n (int): Die Position des gewünschten Dokuments (1-basiert).
    """
    # 1. Eingabe validieren
    if not isinstance(n, int) or n < 1:
        print("Fehler: Bitte geben Sie eine positive ganze Zahl für 'n' an.")
        return

    # 2. Ordner 'examples' prüfen und ggf. erstellen
    output_dir = "examples"
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        print(f"Fehler beim Erstellen des Verzeichnisses '{output_dir}': {e}")
        return

    # 3. Datenbankabfrage
    try:
        collection = connect_to_mongo()

        # Finde das n-te Dokument, indem n-1 Dokumente übersprungen werden
        # `.find()` gibt einen Cursor zurück, `.next()` holt das erste (und einzige) Element
        cursor = collection.find({"language": language}).skip(n - 1).limit(1)
        document = next(cursor, None)  # Gibt 'None' zurück, wenn der Cursor leer ist

        if not document:
            print(
                f"Fehler: Es konnte kein {n}-tes {language} Urteil gefunden werden. (Existieren weniger als {n}?)")
            return

        # 4. JSON-Datei schreiben
        # Konvertiere die MongoDB ObjectId in einen String, damit sie JSON-kompatibel ist
        if '_id' in document and isinstance(document['_id'], ObjectId):
            document['_id'] = str(document['_id'])

        file_path = os.path.join(output_dir, f"{language}_judgment_{n}.json")

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(document, f, ensure_ascii=False, indent=4)

        print(f"✅ Erfolg! Das {n}-te {language} Urteil wurde in der Datei '{file_path}' gespeichert.")

    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")


if __name__ == "__main__":
    # Beispielaufruf: Speichere das 5. vietnamesische Urteil
    element_nummer = 5
    get_nth_judgment(n=element_nummer, language="English")
