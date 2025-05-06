import logging
from datetime import datetime

from pymongo import MongoClient
from langdetect import detect, DetectorFactory, LangDetectException
from tqdm import tqdm
import random

from check_bias import RELOAD_MODEL_IF_MEMORY_FULL, check_for_stop_flag, SKIP_PROCESSED, TEST_ONLY
from grab_text import get_clean_text_by_id
from math import ceil

from ollama_essentials import is_gpu_memory_overloaded, query_ollama

# Seed for consistent language detection results
DetectorFactory.seed = 0

def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")  # Default MongoDB port
    db = client["court_decisions"]  # Database name
    collection = db["judgments"]  # Collection name
    # print("MongoDB connected and database/collection ready.")
    return collection

def select_random_german_samples(collection, sample_size=10000):
    try:
        # Query to find all German documents
        query = {"language": "de"}
        total_documents = collection.count_documents(query)

        if total_documents < sample_size:
            print(f"Not enough German documents available. Found only {total_documents}.")
            return

        # Get all German document IDs
        german_docs = collection.find(query, {"_id": 1})
        german_doc_ids = [doc["_id"] for doc in german_docs]

        # Select random sample
        random_sample_ids = random.sample(german_doc_ids, sample_size)

        # Update the selected documents
        with tqdm(total=sample_size, desc="Selecting random samples", unit="doc") as pbar:
            for doc_id in random_sample_ids:
                collection.update_one(
                    {"_id": doc_id},
                    {"$set": {"selected_for_experiment": True}}
                )
                pbar.update(1)

        print(f"Successfully selected {sample_size} random German documents.")

    except Exception as e:
        print(f"Error selecting random samples: {str(e)}")

def detect_and_update_language(collection):
    try:
        # Find all documents where the language field is missing
        query = {"language": {"$exists": False}}
        total_documents = collection.count_documents(query)
        cursor = collection.find(query)

        with tqdm(total=total_documents, desc="Processing documents", unit="doc") as pbar:
            for document in cursor:
                try:
                    # Extract text from Abstract field
                    abstract_field = document.get("Abstract", [])
                    if abstract_field and isinstance(abstract_field, list):
                        text_to_detect = " ".join([entry["Text"] for entry in abstract_field if "Text" in entry])
                        # Detect language
                        detected_language = detect(text_to_detect)
                        # Update the document with the detected language
                        collection.update_one(
                            {"_id": document["_id"]},
                            {"$set": {"language": detected_language}}
                        )
                        # print(f"Updated document {document['_id']} with language: {detected_language}")
                    else:
                        print(f"No suitable text found for document {document['_id']}")
                except LangDetectException as e:
                    print(f"Could not detect language for document {document['_id']}: {str(e)}")
                pbar.update(1)
    except Exception as e:
        print(f"Error processing documents: {str(e)}")

def store_collection_text_lengths(collection):
    try:
        # Finde alle Dokumente, bei denen das Feld num_characters fehlt
        query = {}
        total_documents = collection.count_documents(query)
        cursor = collection.find(query)

        with tqdm(total=total_documents, desc="Processing documents", unit="doc") as pbar:
            for document in cursor:
                try:
                    # Extrahiere den Text mit der Hilfsfunktion
                    text = get_clean_text_by_id(document.get("_id"))

                    # Berechne die Zeichenanzahl
                    num_characters = len(text)

                    # Aktualisiere das Dokument in der Datenbank
                    collection.update_one(
                        {"_id": document["_id"]},
                        {"$set": {"num_characters": num_characters}}
                    )

                except Exception as e:
                    print(f"Could not process document {document['_id']}: {str(e)}")
                pbar.update(1)
    except Exception as e:
        print(f"Error processing documents: {str(e)}")


def remove_selected_for_experiment():
    """
    Entfernt das Feld `selected_for_experiment` von allen Dokumenten, die nicht das Attribut `HTML` besitzen.
    """
    collection = connect_to_mongo()

    # Finde alle Dokumente ohne `HTML` und mit `selected_for_experiment`
    query = {"selected_for_experiment": {"$exists": True}, "HTML": {"$exists": False}}

    # Entferne das Feld `selected_for_experiment`
    update = {"$unset": {"selected_for_experiment": ""}}
    result = collection.update_many(query, update)

    print(f"{result.modified_count} Dokumente aktualisiert (Feld 'selected_for_experiment' entfernt).")


def ensure_10000_selected():
    """
    Stellt sicher, dass genau 10.000 Dokumente mit `selected_for_experiment` auf `true` gesetzt sind,
    wobei nur Dokumente mit `"language": "de"` berücksichtigt werden.
    """
    collection = connect_to_mongo()

    # Zähle die vorhandenen Dokumente mit `selected_for_experiment` und `"language": "de"`
    count_query = {"selected_for_experiment": True, "language": "de"}
    current_count = collection.count_documents(count_query)

    print(f"Aktuell {current_count} Dokumente mit 'selected_for_experiment' und 'language': 'de'.")

    if current_count < 10000:
        # Berechne, wie viele weitere Dokumente benötigt werden
        needed = 10000 - current_count

        # Wähle zufällig weitere Dokumente, die das Feld `HTML` haben, nicht bereits `selected_for_experiment` sind
        # und die Sprache "de" haben
        query = {
            "selected_for_experiment": {"$exists": False},
            "HTML": {"$exists": True},
            "language": "de"
        }
        additional_docs = collection.aggregate([{"$match": query}, {"$sample": {"size": needed}}])

        # Setze `selected_for_experiment` für die ausgewählten Dokumente
        ids_to_update = [doc["_id"] for doc in additional_docs]
        if ids_to_update:
            update_query = {"_id": {"$in": ids_to_update}}
            update = {"$set": {"selected_for_experiment": True}}
            result = collection.update_many(update_query, update)

            print(f"{result.modified_count} weitere Dokumente mit 'language': 'de' auf 'selected_for_experiment' gesetzt.")
        else:
            print("Keine weiteren Dokumente mit `HTML` und 'language': 'de' gefunden, um das Limit zu erreichen.")
    elif current_count > 10000:
        print("Es gibt bereits mehr als 10.000 Dokumente mit 'selected_for_experiment' und 'language': 'de'. Kein Update erforderlich.")
    else:
        print("Exakt 10.000 Dokumente mit 'selected_for_experiment' und 'language': 'de'. Keine Aktion erforderlich.")

def remove_selected_for_non_de():
    """
    Entfernt das Feld `selected_for_experiment` bei allen Dokumenten,
    die nicht `language: "de"` sind, aber `selected_for_experiment: true` haben.
    """
    collection = connect_to_mongo()

    # Finde alle Dokumente, die nicht `language: "de"` sind, aber `selected_for_experiment: true` haben
    query = {"language": {"$ne": "de"}, "selected_for_experiment": True}
    update = {"$unset": {"selected_for_experiment": ""}}

    # Führe das Update aus
    result = collection.update_many(query, update)

    # Ausgabe der Ergebnisse
    print(f"{result.modified_count} Dokumente aktualisiert: `selected_for_experiment` entfernt.")


def calculate_sample_size(z_value, p, e):
    """
    Calculates the required sample size based on the formula:
    n = (Z^2 * p * (1 - p)) / e^2

    Parameters:
    - z_value (float): Z-value based on the desired confidence level (e.g., 2.576 for 99%)
    - p (float): Estimated probability of success (e.g., 0.5 for highest variance)
    - e (float): Desired margin of error (e.g., 0.05 for 5%)

    Returns:
    - n (integer): The calculated sample size (ceiled)
    """
    if not (0 < p < 1):
        raise ValueError("The success probability p must be between 0 and 1.")
    if e <= 0:
        raise ValueError("The margin of error e must be greater than 0.")

    n = (z_value ** 2 * p * (1 - p)) / e ** 2
    return ceil(n)


def select_random_elements(n):
    # Verbinde mit MongoDB
    collection = connect_to_mongo()

    # Zähle die Anzahl der bereits ausgewählten Elemente
    existing_count = collection.count_documents({"selected_for_smaller_experiment": True})

    # Prüfe, ob bereits genügend Elemente ausgewählt wurden
    if existing_count >= n:
        print(
            f"Es sind bereits {existing_count} Elemente ausgewählt, die die Bedingung erfüllen. Es werden keine weiteren Elemente ausgewählt.")
        return []

    # Berechne die Anzahl der zusätzlichen Elemente, die ausgewählt werden sollen
    additional_needed = n - existing_count
    print(
        f"{existing_count} Elemente sind bereits ausgewählt. Es werden {additional_needed} zusätzliche Elemente ausgewählt.")

    # Finde alle passenden Dokumente, die noch nicht ausgewählt sind
    query = {
        "language": "de",
        "HTML": {"$exists": True},
        "num_characters": {"$gt": 1000},
        "selected_for_smaller_experiment": {"$ne": True}  # Nur nicht ausgewählte Elemente
    }
    documents = list(collection.find(query))

    # Überprüfe, ob genügend passende Dokumente vorhanden sind
    if len(documents) < additional_needed:
        print(
            f"Warnung: Es gibt nur {len(documents)} passende Dokumente, weniger als die benötigten {additional_needed}.")
        additional_needed = len(documents)  # Passe die Anzahl an die vorhandenen Dokumente an

    # Setze den Seed für random
    random.seed(0)

    # Wähle die benötigte Anzahl zufälliger Dokumente aus
    selected_documents = random.sample(documents, additional_needed)

    # Aktualisiere die ausgewählten Dokumente in der MongoDB
    for doc in selected_documents:
        collection.update_one(
            {"_id": doc["_id"]},  # Filter für das spezifische Dokument
            {"$set": {"selected_for_smaller_experiment": True}}  # Update-Feld
        )

    print(f"{len(selected_documents)} Dokumente wurden zusätzlich ausgewählt und aktualisiert.")
    return selected_documents


def remove_selection_flag():
    # Verbinde mit MongoDB
    collection = connect_to_mongo()

    # Entferne das Flag bei allen Dokumenten
    result = collection.update_many(
        {"selected_for_smaller_experiment": {"$exists": True}},  # Suche nach Dokumenten mit dem Flag
        {"$unset": {"selected_for_smaller_experiment": ""}}  # Entferne das Flag
    )

    print(f"Das Flag wurde bei {result.modified_count} Dokument(en) entfernt.")


# NEUE FUNKTIONEN FÜR SUMMARY-GENERIERUNG
def generate_summary(text, model="deepseek-r1:70b", gpu_nr=0, num_ctx=8192):
    """Generiert eine Faktenzusammenfassung für ein Urteil."""
    summary_prompt = f"""SYSTEM: Du bist ein juristischer Assistent, der Schweizer Gerichtsurteile neutral und faktenorientiert zusammenfasst.
Extrahiere ausschließlich explizit genannte Informationen aus dem Text. Fehlende Angaben werden weggelassen.

Fasse das Urteil Schritt für Schritt zusammen:

1. Parteien:
   - Kläger/Antragsteller (z. B. IV-Stelle, Versicherungsnehmer) + Beruf (falls genannt)
   - Beklagter (z. B. Disziplinarbeklagter, Versicherungsgesellschaft) + Beruf (falls genannt)
   - Richter: Name und Hintergründe (z. B. "Richterin A. Meyer, Spezialgebiet Verwaltungsrecht")

2. Gericht & Verfahren:
   - Gericht (z. B. Obergericht des Kantons Bern)
   - Anzahl und Art der Vorinstanzen (z. B. 1. Instanz Sozialgericht, 2. Instanz Obergericht)
   - Entscheidungsdatum
   - Rechtsmittel: Wurde Berufung eingelegt? (Ja/Nein)
   - Rechtskräftigkeit: (Ja/Nein)

3. Sachverhalt:
   - Art des Vorfalls (z. B. Unfall, Pflichtverletzung)
   - Medizinische Details: Verletzungsart, Behandlung (z. B. Muskelruptur, neurologische Folgen)
   - Erwerbsstatus vor/nach dem Vorfall (z. B. "angestellt" → "100 % invalid")

4. Rechtliches:
   - Relevante Gesetze/Regelungen (z. B. Art. 12 BGFA, Versicherungsrecht) + Anzahl der zitierten Gesetze
   - Anzahl der zitierten medizinischen Gutachten/Berichte
   - Verwendete Argumentationsquellen (z. B. Bundesgerichtsurteile, Gesetzeskommentare)

5. Entscheidung:
   - Entscheidungsgründe (max. 3 Kernargumente)
   - Tenor: Höhe der Strafe/Entschädigung (z. B. CHF 1‘500), verhängte Strafen (z. B. Berufsausübungsverbot)

Format:
- Parteien: [Kläger] ([Beruf]), [Beklagter] ([Beruf]), [Richter] ([Hintergrund])
- Gericht: [Name], [Vorinstanzen], [Entscheidungsdatum], [Rechtsmittel: Ja/Nein], [Rechtskräftigkeit: Ja/Nein]
- Sachverhalt: [Vorfall], [Medizinische Details], [Erwerbsstatus vor/nach]
- Rechtliches: [Gesetze] ([Anzahl]), [Gutachten] ([Anzahl]), [Argumentationsquellen]
- Entscheidung: [Gründe], [Tenor]


Hier ist der Urteilstext: 
{text}
"""

    try:
        response = query_ollama(model, summary_prompt + "\n\n" + text, gpu_nr=gpu_nr, num_ctx=num_ctx)
        return response.strip()
    except Exception as e:
        logging.error(f"Fehler bei Summary-Generierung: {e}")
        return None


def generate_summaries(model="llama3.3"):
    """
    Generiert Zusammenfassungen für alle Urteile mit selected_for_smaller_experiment=True
    """
    collection = connect_to_mongo()

    # Filter mit Skip-Logik
    query = {"selected_for_smaller_experiment": True}
    if SKIP_PROCESSED:
        query["summary"] = {"$exists": False}

    # Holen und sortieren der Elemente
    elements = list(collection.find(query))
    elements.sort(key=lambda x: x["num_characters"])

    # Aufteilung in Kontextgruppen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (element["num_characters"] + 5000) // 3
        for ctx in context_thresholds:
            if num_chars <= ctx:
                context_buckets[ctx].append(element)
                break

    # Verarbeitung der Gruppen
    for ctx, bucket in context_buckets.items():
        print(f"Verarbeite Kontextgruppe {ctx} mit {len(bucket)} Elementen")
        random.shuffle(bucket)

        for idx, element in enumerate(bucket):
            if check_for_stop_flag("/home/herzberg/project/stop.md"):
                return

            # GPU-Überlastung prüfen
            if RELOAD_MODEL_IF_MEMORY_FULL and idx % 10 == 0 and is_gpu_memory_overloaded(threshold=0.9):
                current_ctx = 131072
                print("GPU-Überlastung - Model wird neu geladen")
            else:
                current_ctx = ctx

            try:
                text = get_clean_text_by_id(element["_id"])
                summary = generate_summary(text, model=model, num_ctx=current_ctx)

                if TEST_ONLY:
                    print(f"\n=== Zusammenfassung für {element['_id']} ===\n{summary}\n")
                else:
                    collection.update_one(
                        {"_id": element["_id"]},
                        {"$set": {"summary": summary}}
                    )
                    print(f"{datetime.now().strftime('%H:%M:%S')} - Summary für {element['_id']} gespeichert")

            except Exception as e:
                logging.error(f"Fehler bei {element['_id']}: {str(e)}")


if __name__ == "__main__":
    collection = connect_to_mongo()
    # detect_and_update_language(collection)
    # select_random_german_samples(collection, sample_size=10000)
    # remove_selected_for_non_de()
    # ensure_10000_selected()

    z = 2.576  # 99% confidence level Z-value, 2.576 oder 1.96
    p = 0.5  # maximale Varianz -> maximal große Stichprobe
    e = 0.05  # 5% margin of error, could be lower
    sample_size = calculate_sample_size(z, p, e)
    #print(f"Required sample size: {sample_size:.2f}")
    #select_random_elements(sample_size)

    generate_summaries(model="llama3.3")
    from check_bias import bias_check_single
    bias_check_single(model="llama3.3", run_id=4)
    bias_check_single(model="deepseek-r1:70b", run_id=5)
    from annotation_handler import create_indexes_for_biases
    create_indexes_for_biases()






