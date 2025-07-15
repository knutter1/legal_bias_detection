import logging
from datetime import datetime

from pymongo import MongoClient
from langdetect import detect, DetectorFactory, LangDetectException
from tqdm import tqdm
import random

from check_bias import RELOAD_MODEL_IF_MEMORY_FULL, check_for_stop_flag
from grab_text import clean_text
from math import ceil

from ollama_essentials import is_gpu_memory_overloaded, query_ollama

TEST_ONLY = False
SKIP_PROCESSED = True

def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")  # Default MongoDB port
    db = client["court_decisions"]  # Database name
    collection = db["judgments"]  # Collection name
    # print("MongoDB connected and database/collection ready.")
    return collection

# NEUE FUNKTIONEN FÜR SUMMARY-GENERIERUNG
def generate_summary_english(text, model="llama3.1", gpu_nr=0, num_ctx=8192):
    """Generiert eine Faktenzusammenfassung für ein Urteil."""
    summary_prompt = f"""SYSTEM: You are a legal assistant who summarises Swiss court rulings in a neutral and fact-based manner.
Extract only explicitly stated information from the text. Omit any missing information.

Summarise the ruling step by step:

1. Parties:
   - Plaintiff/applicant (e.g. IV office, policyholder) + occupation (if mentioned)
   - Defendant (e.g. disciplinary defendant, insurance company) + occupation (if mentioned)
   - Judge: Name and background (e.g. ‘Judge A. Meyer, specialising in administrative law’)

2. Court & proceedings:
   - Court (e.g. High Court of the Canton of Bern)
   - Number and type of lower courts (e.g. 1st instance social court, 2nd instance high court)
   - Date of decision
   - Legal remedy: Was an appeal lodged? (Yes/No)
   - Legal validity: (Yes/No)

3. Facts:
   - Type of incident (e.g. accident, breach of duty)
   - Medical details: Type of injury, treatment (e.g. muscle rupture, neurological consequences)
   - Employment status before/after the incident (e.g. ‘employed’ → ‘100% disabled’)

4. Legal aspects:
   - Relevant laws/regulations (e.g. Art. 12 BGFA, insurance law) + number of laws cited
   - Number of medical opinions/reports cited
   - Sources of argumentation used (e.g. Federal Court rulings, legal commentaries)

5. Decision:
   - Reasons for the decision (max. 3 key arguments)
   - Tenor: Amount of the penalty/compensation (e.g. CHF 1,500), penalties imposed (e.g. ban on practising profession)

Format:
- Parties: [Plaintiff] ([profession]), [Defendant] ([profession]), [Judge] ([background])
- Court: [Name], [Lower courts], [Date of decision], [Appeal: Yes/No], [Legal validity: Yes/No]
- Facts: [incident], [medical details], [employment status before/after]
- Legal: [laws] ([number]), [expert opinions] ([number]), [sources of argumentation]
- Decision: [reasons], [tenor]


Here is the text of the judgment:
{text}
"""

    try:
        response = query_ollama(model, summary_prompt, gpu_nr=gpu_nr, num_ctx=num_ctx)

        return response.strip()
    except Exception as e:
        logging.error(f"Fehler bei Summary-Generierung: {e}")
        return None


def generate_summaries(model="llama3.1"):
    """
    Generiert Zusammenfassungen für alle Urteile mit selected_for_smaller_experiment=True
    """
    collection = connect_to_mongo()

    # Filter mit Skip-Logik
    query = {"selected_for_annotation": True, "language": "English"}
    if SKIP_PROCESSED:
        query["summary"] = {"$exists": False}

    # Holen und sortieren der Elemente
    elements = list(collection.find(query))
    elements.sort(key=lambda x: len(x["full_text"]) )

    # Aufteilung in Kontextgruppen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (len(element["full_text"]) + 5000) // 3
        for ctx in context_thresholds:
            if num_chars <= ctx:
                context_buckets[ctx].append(element)
                break

    # Verarbeitung der Gruppen
    for ctx, bucket in context_buckets.items():
        print(f"Verarbeite Kontextgruppe {ctx} mit {len(bucket)} Elementen")
        random.shuffle(bucket)

        for idx, element in enumerate(bucket):


            # GPU-Überlastung prüfen
            if RELOAD_MODEL_IF_MEMORY_FULL and idx % 10 == 0 and is_gpu_memory_overloaded(threshold=0.9):
                current_ctx = 131072
                print("GPU-Überlastung - Model wird neu geladen")
            else:
                current_ctx = ctx

            try:
                text = clean_text(element["full_text"])
                summary = generate_summary_english(text, model=model, num_ctx=current_ctx)

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


if __name__ == '__main__':
    generate_summaries(model="llama3.1")