#!/usr/bin/env python3
"""
vietnamese_add_summaries.py
-------------------------------------------------
•  Sucht alle Urteile
     - selected_for_annotation = True
     - language               = "Vietnamese"
     - ollama_responses.response.biases  existiert
•  Trägt in jedem Bias-Objekt die Dokument-Summary
   nach, falls dort summary == None (oder fehlt).
•  Gibt jede geänderte Bias-Struktur zur Kontrolle
   auf der Konsole aus.
•  Schreibt nur, wenn TEST_ONLY = False.
-------------------------------------------------
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from bson import ObjectId
from pymongo import MongoClient

# ---------------- Konfiguration -----------------
TEST_ONLY  = True                 # False  → DB-Schreibzugriff
MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "court_decisions"
COLL_NAME  = "judgments"
# ------------------------------------------------


def connect_to_mongo() -> "pymongo.collection.Collection":
    """
    Einfache Verbindungs­funktion; angelehnt an das Muster
    in deinen bestehenden Skripten. :contentReference[oaicite:0]{index=0}
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    db     = client[DB_NAME]
    return db[COLL_NAME]


def safe_bias_list(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Holt resp['response']['biases'] robust – unabhängig davon, ob
    resp['response'] bereits ein Dict ist, ein JSON-String oder gar
    etwas anderes.
    """
    raw = resp.get("response")

    # 1) Schon ein Dict?
    if isinstance(raw, dict):
        return raw.get("biases", [])

    # 2) JSON-String mit Biases?
    if isinstance(raw, str):
        try:
            maybe_dict = json.loads(raw)
            if isinstance(maybe_dict, dict):
                return maybe_dict.get("biases", [])
        except (ValueError, TypeError):
            pass  # kein JSON – ignorieren

    # 3) Fallback: keine Biases
    return []


def update_bias_entries():
    coll       = connect_to_mongo()
    query = {
        "selected_for_annotation": True,
        "language": "Vietnamese",
        "ollama_responses.response.biases": {"$exists": True},
    }

    total, changed = 0, 0

    for doc in coll.find(query):
        total       += 1
        doc_summary  = doc.get("summary")
        if not doc_summary:
            continue                           # nichts zu kopieren

        set_ops: Dict[str, Any]   = {}
        print_blocks: List[Dict]  = []

        for ridx, resp in enumerate(doc.get("ollama_responses", [])):
            for bidx, bias in enumerate(safe_bias_list(resp)):
                if bias.get("summary") is None:
                    path           = f"ollama_responses.{ridx}.response.biases.{bidx}.summary"
                    set_ops[path]  = doc_summary

                    # Vorschau erzeugen
                    new_bias       = bias.copy()
                    new_bias["summary"] = doc_summary
                    print_blocks.append(new_bias)

        # ---------------- Write / Dry-Run ----------------
        if set_ops:
            changed += 1
            header = (
                "[TEST-ONLY]" if TEST_ONLY else
                datetime.now().strftime("%H:%M:%S")
            )
            print(f"\n{header} – Dokument {doc.get('id', '?')}"
                  f" / {str(doc['_id'])[:8]} wird aktualisiert:")

            for block in print_blocks:
                print(json.dumps(block, ensure_ascii=False, indent=2))

            if not TEST_ONLY:
                coll.update_one({"_id": doc["_id"]}, {"$set": set_ops})

    # ---------------- Zusammenfassung --------------------
    print(
        f"\nFERTIG – geprüft: {total} Dokumente, "
        f"aktualisiert: {changed}  "
        f"({'nur Testlauf' if TEST_ONLY else 'in DB geschrieben'})"
    )


if __name__ == "__main__":
    update_bias_entries()
