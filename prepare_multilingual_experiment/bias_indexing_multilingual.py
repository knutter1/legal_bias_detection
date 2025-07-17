"""
bias_indexing_multilingual.py

– Vollständig überarbeitete Fassung –
(enthält Unicode-Normalisierung, sprach­abhängige Bias-Listen
und eindeutige kanonische IDs für alle Sprachen)

Benötigt:
    pip install pymongo
    prepare_data.connect_to_mongo  (muss die Mongo-Collection liefern)
"""

from prepare_data import connect_to_mongo
import unicodedata
import re
import time
from typing import List, Dict

# ---------------------------------------------------------------------------
# 1) Kanonische (englische) Bias-Liste  – Reihenfolge definiert die ID
# ---------------------------------------------------------------------------
CANONICAL_BIASES: List[str] = [
    "no bias",
    "gender bias",
    "religious bias",
    "racial bias",
    "sexual orientation bias",
    "age discrimination",
    "nationality bias",
    "disability bias",
    "appearance bias",
    "socioeconomic status bias",
    "invalid response structure",
]

# ---------------------------------------------------------------------------
# 2) Übersetzungen in jede Sprache  (Unicode-normalisiert & lower-case!)
# ---------------------------------------------------------------------------
def _nfc_lc(s: str) -> str:
    """Helper: NFC-normalisieren + lowercase + trim."""
    return unicodedata.normalize("NFC", s).strip().lower()


TRANSLATIONS: Dict[str, Dict[str, str]] = {
    "English": {_nfc_lc(b): b for b in CANONICAL_BIASES},
    "Vietnamese": {
        _nfc_lc("không có thiên kiến"): "no bias",
        _nfc_lc("thiên kiến giới tính"): "gender bias",
        _nfc_lc("thiên kiến tôn giáo"): "religious bias",
        _nfc_lc("thiên kiến chủng tộc"): "racial bias",
        _nfc_lc("thiên kiến xu hướng tình dục"): "sexual orientation bias",
        _nfc_lc("phân biệt tuổi tác"): "age discrimination",
        _nfc_lc("thiên kiến quốc tịch"): "nationality bias",
        _nfc_lc("thiên kiến đối với người khuyết tật"): "disability bias",
        _nfc_lc("thiên kiến ngoại hình"): "appearance bias",
        _nfc_lc("thiên kiến địa vị kinh tế xã hội"): "socioeconomic status bias",
        _nfc_lc("cấu trúc phản hồi không hợp lệ"): "invalid response structure",
    },
    "Japanese": {
        _nfc_lc("バイアスなし"): "no bias",
        _nfc_lc("ジェンダーバイアス"): "gender bias",
        _nfc_lc("宗教バイアス"): "religious bias",
        _nfc_lc("人種バイアス"): "racial bias",
        _nfc_lc("性的指向バイアス"): "sexual orientation bias",
        _nfc_lc("年齢差別"): "age discrimination",
        _nfc_lc("国籍バイアス"): "nationality bias",
        _nfc_lc("障害者バイアス"): "disability bias",
        _nfc_lc("外見バイアス"): "appearance bias",
        _nfc_lc("社会経済的地位バイアス"): "socioeconomic status bias",
        _nfc_lc("無効な応答構造"): "invalid response structure",
    },
    # deutsch oder weitere Sprachen können hier ergänzt werden
}

# ---------------------------------------------------------------------------
# 3) Lauf-ID-Mapping (unverändert)
# ---------------------------------------------------------------------------
LANGUAGE_RUN_ID_MATCHES = {
    "de": [6],
    "English": [9],
    "Vietnamese": [10],
    "Japanese": [11],
}

#   –  [:：]  akzeptiert westliche und japanische Doppelpunkte.
#   –  \s*    schluckt Leerzeichen/Tabulatoren.
#   –  (?=\n{2,}...)  stoppt am nächsten Block oder am Dateiende.
#   –  re.IGNORECASE nur bei Vietnamesisch, weil Groß-/Kleinschreibung variiert.

PATTERNS = {
    "English": re.compile(
        r'Identified Bias[:：]\s*(.*?)\s*\n'
        r'(?:Text Passage|Text passage)[:：]\s*(.*?)\s*\n'
        r'(?:Justification|Reasoning)[:：]\s*(.*?)(?=\n{2,}(?:Identified Bias|検出されたバイアス|Thiên kiến)|\Z)',
        re.DOTALL,
    ),

    "Japanese": re.compile(
        r'検出されたバイアス[:：]\s*(.*?)\s*\n'
        r'(?:テキスト|本文|抜粋)[:：]\s*(.*?)\s*\n'
        r'(?:根拠|理由|正当化)[:：]\s*(.*?)(?=\n{2,}(?:検出されたバイアス|Identified Bias|Thiên kiến)|\Z)',
        re.DOTALL,
    ),

    "Vietnamese": re.compile(
        r'Thiên kiến(?: đã)? nhận dạng[:：]\s*(.*?)\s*\n'
        r'(?:Đoạn văn bản|Trích dẫn)[:：]\s*(.*?)\s*\n'
        r'(?:Lý do|Giải thích)[:：]\s*(.*?)(?=\n{2,}(?:Thiên kiến|Identified Bias|検出されたバイアス)|\Z)',
        re.DOTALL | re.IGNORECASE,
    ),
}


def MATCHING_SET(lang: str):
    """Welche run_ids sollen für eine Sprache ausgewertet werden?"""
    return LANGUAGE_RUN_ID_MATCHES.get(lang, [])


# ---------------------------------------------------------------------------
# 4) Hilfsfunktionen (Normalisierung & Lookup)
# ---------------------------------------------------------------------------
def normalize(text: str) -> str:
    """
    Unicode-NFC, trim, lower-case.
    Verwenden wir überall, damit Vergleiche robust sind.
    """
    return unicodedata.normalize("NFC", text).strip().lower()


def to_canonical(language: str, bias_str: str) -> str | None:
    """
    Übersetzt einen sprach­spezifischen Bias-String zur kanonischen
    englischen Bezeichnung (oder None, falls unbekannt).
    """
    mapping = TRANSLATIONS.get(language)
    if mapping is None:
        return None
    return mapping.get(normalize(bias_str))


# ---------------------------------------------------------------------------
# 5) Hauptfunktion
# ---------------------------------------------------------------------------
TEST_ONLY = True   # Flag für Trockenlauf


def create_indexes_for_biases(language: str):
    """
    Lies die Urteile der angegebenen Sprache aus Mongo, parse Bias-Blöcke
    und schreibe (optional) zurück.
    """
    collection = connect_to_mongo()
    run_ids = MATCHING_SET(language)

    print(f"run_ids for {language}: {run_ids}")
    query = {"selected_for_annotation": True, "language": language, "ollama_responses": {"$exists": True} }
    print(f"Anzahl der judgments: {collection.count_documents(query)}")

    biases: list[dict] = []
    seen: set[tuple[str, str]] = set()  # (canonical_bias, textpassage)

    # ------------------------------------------------------------
    for judgment in collection.find(query):
        for run_id in run_ids:
            for response in judgment["ollama_responses"]:
                if response.get("run_id") != run_id:
                    continue

                # Original-Text des Modells holen
                content = (
                    response["response"]["original_text"]
                    if isinstance(response["response"], dict)
                    else response["response"]
                )

                # Prompt-Gedanken abschneiden
                content = content.split("</think>")[-1].lstrip()

                # schneller Exit, falls explizit „No bias“ / Äquivalent
                if normalize(content) in {
                    _nfc_lc("No bias"),
                    _nfc_lc("Không có thiên kiến"),
                    _nfc_lc("バイアスなし"),
                }:
                    continue

                # --------------------------------------------------------
                # Im DB-Speicherformat Original- & Parsed-Version trennen
                # --------------------------------------------------------
                if not TEST_ONLY and isinstance(response["response"], str):
                    collection.update_one(
                        {"_id": judgment["_id"], "ollama_responses.run_id": run_id},
                        {"$set": {
                            "ollama_responses.$.response": {
                                "original_text": response["response"],
                                "biases": []
                            }
                        }}
                    )

                # Bias-Abschnitte finden
                bias_sections = re.findall(
                    r'Identified Bias:\s*(.*?)\n'
                    r'Text Passage:\s*(.*?)\n'
                    r'Justification:\s*(.*?)(?=\n\nIdentified Bias:|\Z)',
                    content,
                    re.DOTALL,
                )

                if not bias_sections:
                    print(f"⚠️  Keine parsbaren Bias-Blöcke in run {run_id} ({judgment['_id']})")
                    print(content)
                    continue

                for raw_bias_type, textpassage, reasoning in bias_sections:
                    # Trim
                    bias_type = raw_bias_type.strip()
                    textpassage = textpassage.strip()
                    reasoning = reasoning.strip()

                    canonical = to_canonical(language, bias_type)
                    if canonical is None:
                        print(f"⚠️  »{bias_type}« is not a valid bias type")
                        continue

                    canon_norm = normalize(canonical)

                    # Duplikat-Check (kanonisch!)
                    key = (canon_norm, textpassage)
                    if key in seen:
                        continue
                    seen.add(key)

                    bias_dict = {
                        "id": len(biases) + 1,
                        "summary": judgment.get("summary"),
                        "origin_url": judgment.get("origin_url"),
                        "run_id": run_id,
                        "bias_type_id": CANONICAL_BIASES.index(canonical),
                        "bias_type_name": canonical,   # englische Kurzform
                        "textpassage": textpassage,
                        "reasoning": reasoning,
                        "annotations": [],
                    }

                    # In DB zurückschreiben
                    if not TEST_ONLY:
                        collection.update_one(
                            {"_id": judgment["_id"], "ollama_responses.run_id": run_id},
                            {"$addToSet": {
                                "ollama_responses.$.response.biases": bias_dict
                            }}
                        )

                    biases.append(bias_dict)

                    # --- Debug-Ausgabe -----------------------------------
                    print(f"[{bias_dict['id']}] {bias_dict['bias_type_name']}")
                    print(textpassage)
                    print(reasoning + "\n")

    return biases


# ---------------------------------------------------------------------------
# 6) Skript-Entry-Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Beispiel: Urteile parsen
    _ = create_indexes_for_biases("Japanese")
