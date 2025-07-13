from pymongo import MongoClient
from prepare_data import connect_to_mongo  # Wiederverwendung der Verbindungsfunktion


def set_annotation_flag_for_languages(languages):
    """
    Setzt das Attribut 'selected_for_annotation' auf 'true' für alle Dokumente,
    die eine der angegebenen Sprachen haben.

    Args:
        languages (list): Eine Liste von Sprachen (z.B. ["Vietnamese", "Japanese"]),
                          die aktualisiert werden sollen.
    """
    if not isinstance(languages, list) or not languages:
        print("Fehler: Bitte geben Sie eine gültige Liste von Sprachen an.")
        return

    try:
        # Verbindung zur MongoDB herstellen
        collection = connect_to_mongo()
        print("Erfolgreich mit der MongoDB verbunden.")

        # Query, um alle Dokumente mit den entsprechenden Sprachen zu finden
        query = {"language": {"$in": languages}}

        # Update-Operation: Fügt das neue Feld hinzu oder überschreibt es
        update = {"$set": {"selected_for_annotation": True}}

        # Führt das Update für alle passenden Dokumente aus
        print(f"Suche nach Dokumenten mit den Sprachen: {', '.join(languages)}...")
        result = collection.update_many(query, update)

        # Gibt das Ergebnis aus
        print(f"Aktualisierung abgeschlossen. {result.modified_count} Dokument(e) wurden aktualisiert.")

    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")


if __name__ == "__main__":
    # Liste der Zielsprachen
    target_languages = ["Vietnamese", "Japanese"]

    # Führt die Update-Funktion aus
    set_annotation_flag_for_languages(target_languages)