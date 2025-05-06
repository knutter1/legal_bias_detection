import csv
import time

import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm  # Für die Fortschrittsanzeige
from grab_text import create_temp_directory, clean_temp_directory, get_clean_text_by_id
import numpy as np
from check_bias import parse_bias_response
from collections import defaultdict
import re
from transformers import AutoTokenizer
from tokenizers import Tokenizer


def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")  # Default MongoDB port
    db = client["court_decisions"]  # Database name
    collection = db["judgments"]  # Collection name
    # print("MongoDB connected and database/collection created.")
    return collection


def analyze_biases_into_csv():
    collection = connect_to_mongo()

    # Prepare the output CSV file
    output_file = "bias_documents.csv"

    total_documents = collection.count_documents({"ollama_responses": {"$exists": True}})
    bias_count = 0

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["_id", "id", "file_path", "model", "response"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Calculate bias quote
        bias_quote = (bias_count / total_documents) * 100 if total_documents > 0 else 0

        # Write the summary above the header
        csvfile.write(f"Total Documents, Bias Count, Bias Quote\n")
        csvfile.write(f"{total_documents}, {bias_count}, {bias_quote:.2f}%\n")

        # Write the header for the table
        writer.writeheader()

        # Query for documents with 'ollama_responses'
        cursor = collection.find({"ollama_responses": {"$exists": True}})

        # Verwende tqdm für die Fortschrittsanzeige
        with tqdm(total=total_documents, desc="Verarbeitung von Dokumenten") as pbar:
            for document in cursor:
                ollama_responses = document.get("ollama_responses", [])
                pbar.update(1)

                if ollama_responses:
                    first_response = ollama_responses[0]
                    response_text = first_response.get("response", "")

                    if response_text.startswith("Bias"):
                        bias_count += 1

                        # Determine the file path (prioritize HTML if present)
                        pdf_file = document.get("PDF", {}).get("Datei")
                        html_file = document.get("HTML", {}).get("Datei")
                        file_path = html_file or pdf_file

                        # Prepend the URL prefix if file_path is present
                        if file_path:
                            file_path = f"https://entscheidsuche.ch/docs/{file_path}"

                        # Write to CSV
                        writer.writerow({
                            "_id": str(document.get("_id")),
                            "id": document.get("id"),
                            "file_path": file_path,
                            "model": first_response.get("model"),
                            "response": response_text
                        })

    print(f"Analysis complete. Results saved to {output_file}.")


def highlight_text(text, text_to_highlight):
    """Hebt die angegebene Textstelle hervor."""
    highlighted = text.replace(
        text_to_highlight, f"**{text_to_highlight}**"
    )
    return highlighted


def calculate_character_statistics(collection):
    """Ermittelt die niedrigste, höchste, durchschnittliche und mediane Zeichenanzahl."""
    cursor = collection.find({"selected_for_experiment": True}, {"num_characters": 1, "_id": 0})
    num_characters = [doc["num_characters"] for doc in cursor if "num_characters" in doc]

    only_top_1_percent = True
    if only_top_1_percent:
        num_characters.sort(reverse=True)
        num_characters = num_characters[:max(1, len(num_characters) // 100)]

    if not num_characters:
        print("Keine Daten vorhanden.")
        return None

    min_chars = min(num_characters)
    max_chars = max(num_characters)
    avg_chars = sum(num_characters) / len(num_characters)
    median_chars = np.median(num_characters)

    print(f"Min Zeichenanzahl: {min_chars}")
    print(f"Max Zeichenanzahl: {max_chars}")
    print(f"Durchschnittliche Zeichenanzahl: {avg_chars}")
    print(f"Median Zeichenanzahl: {median_chars}")

    return {
        "min": min_chars,
        "max": max_chars,
        "average": avg_chars,
        "median": median_chars
    }

def estimate_tokens(char_count, model="llama"):
    """Schätzt die Anzahl der Tokens basierend auf der Zeichenanzahl."""
    # Modellabhängige Zeichen-zu-Token-Konversionsfaktoren (ungefähr)
    conversion_factors = {
        "llama": 4,        # 1 Token pro 4 Zeichen
        "qwen": 3.6,       # 1 Token pro 3.6 Zeichen
        "openai": 4        # 1 Token pro 4 Zeichen
    }

    factor = conversion_factors.get(model, 4)  # Standard: llama
    return round(char_count / factor)

def get_llama_tokens(text):
    # tokenizer from https://huggingface.co/unsloth/llama-3-8b/blob/main/tokenizer.json
    tokenizer = Tokenizer.from_file("tokenizer.json")
    return len(tokenizer.encode(text))

def get_multiple_llama_tokens(texts):
    """
    Berechnet die Anzahl der Tokens für eine Liste von Texten.

    :param texts: Liste von Texten, die tokenisiert werden sollen
    :return: Liste von Token-Anzahlen (eine Zahl pro Text)
    """
    tokenizer = Tokenizer.from_file("tokenizer.json")

    return [len(tokenizer.encode(text)) for text in texts]


def analyze_token_ratio():
    """
    Analysiert, wie viele Einträge ein Verhältnis von num_characters/3 zu Llama-Tokens haben,
    das entweder kleiner oder größer ist.
    """
    collection = connect_to_mongo()
    entries = collection.find({"selected_for_smaller_experiment": True})

    texts = []
    entry_metadata = []

    # Sammle die Texte und Metadaten für die Batch-Verarbeitung
    for entry in entries:
        try:
            clean_text = get_clean_text_by_id(entry["_id"])
            if not clean_text:
                continue

            texts.append(clean_text)
            entry_metadata.append({
                "num_characters": entry["num_characters"]
            })
        except KeyError as e:
            print(f"Fehlendes Feld bei Eintrag {entry['_id']}: {e}")
        except Exception as e:
            print(f"Fehler bei Verarbeitung {entry['_id']}: {e}")

    # Berechne Tokens für alle Texte auf einmal
    llama_token_counts = get_multiple_llama_tokens(texts)

    # Zähler für größer/kleiner Verhältnisse
    count_greater = 0
    count_smaller = 0

    # Verarbeite die Ergebnisse
    for meta, llama_tokens in zip(entry_metadata, llama_token_counts):
        num_chars = meta["num_characters"]
        char_estimate = num_chars / 3

        if char_estimate > llama_tokens:
            count_greater += 1
        else:
            count_smaller += 1

    # Ergebnisse ausgeben
    print(f"Anzahl der Elemente mit char_estimate > llama_tokens: {count_greater}")
    print(f"Anzahl der Elemente mit char_estimate <= llama_tokens: {count_smaller}")


def get_longest_texts(limit=100):
    """
    Gibt die bereinigten Texte der längsten Elemente zurück, basierend auf num_characters.
    """
    collection = connect_to_mongo()

    # Filtere nach `selected_for_experiment` und sortiere absteigend nach `num_characters`
    longest_entries = collection.find({"selected_for_experiment": True}).sort("num_characters", -1).limit(limit)

    texts = []
    for entry in longest_entries:
        entry_id = entry["_id"]  # MongoDB `_id` Feld
        cleaned_text = get_clean_text_by_id(entry_id)  # Text mithilfe der Funktion bereinigen
        print(f"Ungefähre Textlänge in token: {len(cleaned_text) // 3}")
        if cleaned_text:
            texts.append(cleaned_text)  # Füge bereinigten Text der Liste hinzu

    return texts



def calculate_average_characters(collection):
    """
    Berechnet die durchschnittliche Zeichenanzahl aller Elemente mit "selected_for_experiment": True.

    :param collection: MongoDB-Collection-Objekt.
    :return: Durchschnittliche Zeichenanzahl (float).
    """
    # Filtere alle Dokumente mit "selected_for_experiment": True
    query = {"selected_for_experiment": True}

    # Finde alle passenden Dokumente
    documents = collection.find(query, {"num_characters": 1})  # Nur das "num_characters"-Feld abrufen

    # Initialisiere Variablen für die Berechnung
    total_characters = 0
    count = 0

    for doc in documents:
        num_characters = doc.get("num_characters")
        if num_characters is not None:
            total_characters += num_characters
            count += 1

    # Durchschnitt berechnen
    if count > 0:
        average_characters = total_characters / count
    else:
        average_characters = 0  # Kein Dokument gefunden

    print(f"Durchschnittliche Zeichenanzahl: {average_characters:.2f}")
    return average_characters


def count_responses_with_min_length(run_id=3, min_chars=6000):
    """
    Zählt die Anzahl der Dokumente, in denen eine `ollama_response` mit der angegebenen `run_id`
    eine `response`-Länge größer als `min_chars` hat.

    :param run_id: Die `run_id`, die gefiltert werden soll.
    :param min_chars: Die minimale Länge der `response`, die berücksichtigt werden soll.
    :return: Anzahl der passenden Dokumente.
    """
    # Verbindung zur MongoDB herstellen
    collection = connect_to_mongo()

    # Alle Dokumente laden, die eine ollama_response mit der angegebenen run_id enthalten
    query = {"ollama_responses.run_id": run_id}
    documents = collection.find(query)

    # Statistiken initialisieren
    matching_responses = 0
    total_documents = 0

    # Durch jedes Dokument iterieren
    for doc in tqdm(documents, desc="Verarbeitung der Dokumente", unit="Dokument"):
        total_documents += 1
        ollama_responses = doc.get("ollama_responses", [])

        # Filtere Responses mit der angegebenen run_id
        for response in ollama_responses:
            if response.get("run_id") == run_id:
                response_text = response.get("response", "")
                response_length = len(response_text)

                # Überprüfen, ob die Länge der response die Mindestanzahl überschreitet
                if response_length > min_chars:
                    matching_responses += 1

    print(f"Analyse abgeschlossen:")
    print(f"- Gesamtanzahl der Dokumente: {total_documents}")
    print(f"- Anzahl der responses mit mehr als {min_chars} Zeichen: {matching_responses}")

    return {
        "total_documents": total_documents,
        "matching_responses": matching_responses,
    }


def analyse_response_correlations():
    # Verbinde mit MongoDB
    collection = connect_to_mongo()

    # Finde alle Dokumente mit ollama-responses und run_id=3
    query = {"ollama_responses.run_id": 3}
    documents = list(collection.find(query))

    # Extrahiere relevante Daten
    time_taken_list = []
    num_characters_list = []
    response_length_list = []

    for doc in documents:
        for response in doc.get("ollama_responses", []):
            if response.get("run_id") == 3:
                time_taken = response.get("time_taken", 0)  # In Sekunden
                response_text = response.get("response", "")
                time_taken_list.append(time_taken)
                num_characters_list.append(doc.get("num_characters", 0))  # Anzahl Zeichen
                response_length_list.append(len(response_text))  # Anzahl Zeichen

    # Überprüfen, ob Daten vorhanden sind
    if not time_taken_list:
        print("Keine Daten mit run_id=3 gefunden.")
        return

    # Durchschnittsberechnung
    avg_time_taken = np.mean(time_taken_list)

    # Korrelationen berechnen
    correlation_time_characters = (
        np.corrcoef(time_taken_list, num_characters_list)[0, 1]
        if len(time_taken_list) > 1 else 0
    )
    correlation_time_response_length = (
        np.corrcoef(time_taken_list, response_length_list)[0, 1]
        if len(time_taken_list) > 1 else 0
    )

    # Ausgabe
    print("### Analyse der Ollama-Responses mit run_id=3 ###")
    print(f"Anzahl untersuchter Responses: {len(time_taken_list)}")
    print(f"Durchschnittliche Bearbeitungszeit (time_taken): {avg_time_taken:.2f} Sekunden")
    print()
    print("Korrelationen:")
    print(
        f"- Zwischen Bearbeitungszeit (time_taken, Sekunden) und Anzahl der Zeichen im Dokument (num_characters): {correlation_time_characters:.2f}")
    print(
        f"- Zwischen Bearbeitungszeit (time_taken, Sekunden) und Länge der Response (Anzahl Zeichen): {correlation_time_response_length:.2f}")
    print("##################################################")


def check_summary_errors():
    collection = connect_to_mongo()
    error_summaries = []
    doc_count = 0

    for doc in collection.find():
        doc_count += 1
        summary = doc.get("summary", "")
        if "Error" in summary and len(summary) < 500:
            error_summaries.append({
                "document_id": str(doc["_id"]),
                "summary": summary,
                "length": len(summary)
            })

    print("\nSummary Errors Report:")
    print("=======================")
    for entry in error_summaries:
        print(f"Document ID: {entry['document_id']}")
        print(f"Length: {entry['length']} Zeichen")
        print(f"Summary Excerpt: {entry['summary'][:100]}...\n")

    print(f"Gesamtzahl Summary-Fehler: {len(error_summaries)} in {doc_count} Untersuchungen")


def check_ollama_errors():
    collection = connect_to_mongo()
    error_responses = []
    doc_count = 0

    for doc in collection.find():
        for response in doc.get("ollama_responses", []):
            doc_count += 1
            if response.get("run_id") in ["4", "5"] and "Error" in response.get("response", ""):
                error_responses.append({
                    "document_id": str(doc["_id"]),
                    "response_id": response["id"],
                    "run_id": response["run_id"],
                    "model": response["model"],
                    "error_snippet": response["response"].split("Error")[0][-50:] + "Error..."
                })

    print("\nOllama Response Errors Report:")
    print("===============================")
    for entry in error_responses:
        print(f"Document ID: {entry['document_id']}")
        print(f"Response ID: {entry['response_id']}")
        print(f"Run ID: {entry['run_id']} | Model: {entry['model']}")
        print(f"Error Context: ...{entry['error_snippet']}\n")

    print(f"Gesamtzahl Ollama-Response-Fehler: {len(error_responses)} in {doc_count} Untersuchungen")


import re
from collections import defaultdict


def parse_ollama_responses(run_id=5):
    collection = connect_to_mongo()
    bias_counter = defaultdict(int)

    # Definierte Bias-Klassen
    valid_biases = [
        "Kein Bias",
        "Gender-Bias",
        "Religiöser Bias",
        "Rassistischer Bias",
        "Sexuelle Orientierung Bias",
        "Altersdiskriminierung",
        "Nationalität-Bias",
        "Behinderungen-Bias",
        "Erscheinung-Bias",
        "Bias durch sozioökonomischen Status"
    ]

    # Query für Dokumente mit run_id 4 oder 5
    query = {
        "selected_for_smaller_experiment": True,
        "ollama_responses": {
            "$elemMatch": {
                "run_id": {"$in": [run_id]}
            }
        }
    }

    bias_counting_array = [0 for _ in range(len(valid_biases))]

    for doc in collection.find(query):
        for response in doc['ollama_responses']:
            if response.get('run_id') != run_id:
                continue

            content = response['response']

            # <think>-Block entfernen
            if "</think>" in content:
                content = content.split('</think>')[-1].strip()

            # Fall: Kein Bias
            if content.strip() == "Kein Bias":
                bias_counter["Kein Bias"] += 1
                bias_counting_array[0] += 1  # Ensure it counts
                continue

            # Extrahiere alle Bias-Abschnitte
            bias_sections = re.findall(
                r'Identifizierter Bias: (.*?)\nTextpassage: (.*?)\nBegründung: (.*?)(?=\n\nIdentifizierter Bias:|\Z)',
                content,
                re.DOTALL
            )

            for bias_section in bias_sections:
                if len(bias_section) != 3:
                    print(f"⚠️ Invalid bias section: {bias_section}")  # Debug
                    continue

                bias_type, textpassage, reasoning = bias_section
                bias_type = bias_type.strip()  # Normalize

                # Debugging: Print found bias types
                if bias_type not in valid_biases:
                    print(f"⚠️ Unmatched bias type: '{bias_type}'")

                for i, valid_bias in enumerate(valid_biases):
                    if bias_type == valid_bias:
                        bias_counting_array[i] += 1
                        break  # Stop checking after finding a match

    # Ausgabe der gezählten Bias-Vorkommen
    for i in range(len(valid_biases)):
        print(f"{valid_biases[i]}: {bias_counting_array[i]} mal")


if __name__ == "__main__":
    collection = connect_to_mongo()

    #longest_texts = get_longest_texts(limit=1)
    #for text in longest_texts:
    #    print(text[:1000000] + "\n\n\n")  # Zeige die ersten 200 Zeichen jedes Textes als Vorschau

    # print_analysis_bias_and_processing()
    #get_longest_texts(100)
    #print(f"Durchschnittliche Tokenanzahl: {calculate_average_characters(collection) // 3}")
    #create_excel_with_context(collection)
    #analyze_bias_responses(run_id=3)
    #count_responses_with_min_length(run_id=3, min_chars=4000)

    # Funktionsaufruf
    # analyse_response_correlations()
    #print(parse_ollama_responses(run_id=4))
    print(parse_ollama_responses(run_id=5))

    # analyze_token_ratio()
