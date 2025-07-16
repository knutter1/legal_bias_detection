#!/usr/bin/env python3
"""
generate_summaries_multilingual.py

Generiert faktenbasierte Zusammenfassungen für Urteile in mehreren Sprachen
(Japanisch, Vietnamesisch, Englisch, Deutsch).  Die Prompt‑Vorlagen werden
extern als *.txt‑Dateien vorgehalten.

Abhängigkeiten:
    pip install pymongo
    prepare_data.connect_to_mongo           (liefert die Mongo‑Collection)
    grab_text.clean_text                    (reinigt das Urteil vor dem Prompt)
    ollama_essentials.query_ollama          (Chat‑LLM Wrapper)
    ollama_essentials.is_gpu_memory_overloaded (optional, s. RELOAD_MODEL_IF_MEMORY_FULL)
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from pathlib import Path

from prepare_data import connect_to_mongo        # DB‑Connector
from grab_text import clean_text                 # Text‑Reinigung
from ollama_essentials import (
    query_ollama,
    is_gpu_memory_overloaded,
)

# --------------------------------------------------------------------------- #
# 0) Globale Schalter                                                         #
# --------------------------------------------------------------------------- #
SKIP_PROCESSED = True          # Dokumente mit bereits vorhandener Summary überspringen
TEST_ONLY = False              # True ⇒ keine DB‑Writes, nur Konsole
RELOAD_MODEL_IF_MEMORY_FULL = False  # bei GPU‑Speicherproblemen Modell neu laden

# --------------------------------------------------------------------------- #
# 1) Prompt‑Dateien                                                           #
# --------------------------------------------------------------------------- #
PROMPT_FILES: dict[str, Path | None] = {
    "English":      Path("prepare_multilingual_experiment/prompt_summary_english.txt"),
    "German":       None,  # Prompt ist inline‑Konstante (s.u.)
    "Vietnamese":   Path("prepare_multilingual_experiment/prompt_summary_vietnamese.txt"),
    "Japanese":     Path("prepare_multilingual_experiment/prompt_summary_japanese.txt"),
}

# Deutscher Prompt direkt im Code
GERMAN_PROMPT_TEMPLATE = Path(
    PROMPT_FILES["English"]  # nur als Platzhalter für relative Lage genutzt
).with_name("prompt_summary_german.txt").read_text(encoding="utf-8") \
  if Path("prompt_summary_german.txt").exists() else ""   # optional

# --------------------------------------------------------------------------- #
# 2) Utility‑Funktionen                                                       #
# --------------------------------------------------------------------------- #
def load_prompt(lang: str) -> str:
    """Liest die Prompt‑Vorlage für die gewünschte Sprache ein (utf‑8)."""
    p = PROMPT_FILES.get(lang)
    if p is None:
        raise ValueError(f"No external prompt file configured for '{lang}'")
    if not p.exists():
        raise FileNotFoundError(f"Prompt file not found: {p}")
    return p.read_text(encoding="utf-8")


def bucketize_by_context(docs: list[dict], base_chars: int = 5000) -> dict[int, list]:
    """
    Teilt Dokumente in Buckets anhand der benötigten Kontextgröße (chars/3 + Prompt‑Overhead).
    """
    thresholds = [8192, 16384, 32768, 65536, 131072]
    buckets = {ctx: [] for ctx in thresholds}
    for d in docs:
        need = (len(d["full_text"]) + base_chars) // 1
        for ctx in thresholds:
            if need <= ctx:
                buckets[ctx].append(d)
                break
    return buckets


# --------------------------------------------------------------------------- #
# 3) Core: Zusammenfassung für EIN Dokument                                   #
# --------------------------------------------------------------------------- #
def generate_summary_lang(
    text: str,
    lang: str,
    model: str = "llama3.1",
    gpu_nr: int = 0,
    num_ctx: int = 8192,
) -> str | None:
    """
    Erzeugt eine faktenbasierte Zusammenfassung für die angegebene Sprache.
    """
    try:
        if lang == "German":
            if not GERMAN_PROMPT_TEMPLATE:
                raise RuntimeError("German prompt template not found")
            summary_prompt = GERMAN_PROMPT_TEMPLATE.format(text=text)
        else:
            prompt_template = load_prompt(lang)
            summary_prompt = prompt_template.format(text=text)

        response = query_ollama(model, summary_prompt, gpu_nr=gpu_nr, num_ctx=num_ctx)
        return response.strip()
    except Exception as e:
        logging.error(f"[{lang}] Fehler bei Summary‑Generierung: {e}")
        return None


# --------------------------------------------------------------------------- #
# 4) Batch‑Verarbeitung für eine Sprache                                      #
# --------------------------------------------------------------------------- #
def generate_summaries_for_language(
    lang: str,
    model: str = "llama3.1",
    mongo_flag: str = "selected_for_annotation",
):
    """
    Findet Urteile ohne Summary in der gewünschten Sprache, erzeugt die
    Zusammenfassung und speichert sie im Feld ``summary``.
    """
    collection = connect_to_mongo()

    # --- Mongo‑Filter ------------------------------------------------------- #
    query = {mongo_flag: True, "language": lang}
    if SKIP_PROCESSED:
        query["summary"] = {"$exists": False}

    docs = list(collection.find(query))
    docs.sort(key=lambda d: len(d["full_text"]))  # kürzere zuerst

    buckets = bucketize_by_context(docs)

    for ctx, bucket in buckets.items():
        if not bucket:
            continue
        logging.info(f"[{lang}] Kontext {ctx}: {len(bucket)} Dokumente")
        random.shuffle(bucket)

        for idx, doc in enumerate(bucket):
            # Bei Speicherproblemen auf größten Kontext ausweichen
            cur_ctx = 131072 if (
                RELOAD_MODEL_IF_MEMORY_FULL and idx % 10 == 0
                and is_gpu_memory_overloaded(.9)
            ) else ctx

            text_clean = clean_text(doc["full_text"])
            summary = generate_summary_lang(text_clean, lang, model=model, num_ctx=cur_ctx)

            if summary is None:
                continue

            if TEST_ONLY:
                print(f"\n--- {doc['_id']} ({lang}) ---\n{summary}\n")
            else:
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"summary": summary}}
                )
                logging.info(f"[{lang}] Summary gespeichert für {doc['_id']}")


# --------------------------------------------------------------------------- #
# 5) Entry‑Point                                                              #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Nur Vietnamesisch & Japanisch, wie gewünscht
    generate_summaries_for_language(
        "Vietnamese",
        model="llama3.1",                # anpassen, falls anderes Modell
    )
    generate_summaries_for_language(
        "Japanese",
        model="llama3.1",
    )
