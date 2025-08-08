import re
from ollama_essentials import query_ollama
import random
from collections import Counter, defaultdict
from prepare_data import connect_to_mongo
from typing import Dict, List, Tuple

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
]

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
    "Korean": re.compile(
        r'검출된 편향[:：]\s*(.*?)\s*\n'  # Identified Bias
        r'(?:본문|텍스트)[:：]\s*(.*?)\s*\n'  # Text Passage
        r'(?:근거|이유|정당화)[:：]\s*(.*?)(?=\n{2,}'  # Justification
        r'(?:검출된 편향|Identified Bias|検出されたバイアス|Thiên kiến)|\Z)',
        re.DOTALL,
    ),
}


def classify_bias_type(found_bias: str, model: str = "llama3.1") -> str:
    """
    Gibt für `found_bias` genau einen der CANONICAL_BIASES zurück.
    """
    categories = ", ".join(CANONICAL_BIASES)

    prompt = f"""
SYSTEM
You are an expert multilingual content‑analysis model.

TASK
Classify the following text passage into **exactly one** of these canonical bias categories
(case‑insensitive match; return the string exactly as written below, nothing else):

{categories}

TEXT TO CLASSIFY
\"\"\"{found_bias}\"\"\"

RESPONSE FORMAT
Return only the canonical bias category, no explanations, no punctuation, no additional text.
If the passage is not biased, return "no bias".
"""

    for _ in range(5):
        answer = query_ollama(model_name=model,
                              prompt=prompt,
                              num_ctx=2048,
                              temperature=0.1).strip()
        # match unabhängig von Groß‑/Kleinschreibung prüfen
        for cat in CANONICAL_BIASES:
            if answer.lower() == cat.lower():
                return cat
    return ""



def validate_bias_classifier(
    sample_size: int = 200,
    model: str = "llama3.1",
    seed: int = 0,
    show_passes: bool = False,
) -> Tuple[float, Dict[Tuple[str, str], int]]:

    coll = connect_to_mongo()

    pipeline = [
        {"$match": {
            "selected_for_annotation": True,
            "ollama_responses.response.biases": {"$exists": True},
            "language": {"$ne": "de"},          # DE‑Urteile ausschließen
        }},
        {"$unwind": "$ollama_responses"},
        {"$unwind": "$ollama_responses.response.biases"},
        {"$project": {
            "_id": 0,
            "bias": "$ollama_responses.response.biases",
        }},
        {"$sample": {"size": sample_size}},
    ]

    sample: List[Dict] = list(coll.aggregate(pipeline))
    if not sample:
        raise ValueError("Keine passenden Dokumente gefunden – Prüfe die Filter!")

    random.seed(seed)
    correct = 0
    confusion: defaultdict[Tuple[str, str], int] = defaultdict(int)

    print(f"\n--- Starte Validierung mit {len(sample)} Beispielen (ohne deutsche Urteile) ---\n")

    for idx, entry in enumerate(sample, start=1):
        bias_obj = entry["bias"]

        true_label: str = (bias_obj.get("bias_type_name") or "").strip()
        identified_bias_raw: str = bias_obj.get("identified_bias") or true_label
        pred_label: str = classify_bias_type(identified_bias_raw, model=model)

        # --------------------------------------------------------
        #  Case‑insensitiver Vergleich  ↓↓↓
        # --------------------------------------------------------
        ok = pred_label.lower() == true_label.lower()

        confusion[(true_label, pred_label)] += 1
        if ok:
            correct += 1

        # ---------- Ausgaben ----------
        if ok and show_passes:
            print(f"[{idx:>3}/{sample_size}] ✅  OK  —  {true_label}")
        elif not ok:
            print(
                f"[{idx:>3}/{sample_size}] ❌  FAIL\n"
                f"         GT : '{true_label}'\n"
                f"         KI : '{pred_label}'\n"
                f"  Raw Bias  : \"{identified_bias_raw[:120]}{'…' if len(identified_bias_raw)>120 else ''}\"\n"
            )

    accuracy: float = correct / len(sample)

    print("\n--- Ergebnis ---")
    print(f"Accuracy: {accuracy:.2%}  ({correct}/{len(sample)} korrekt)")

    return accuracy, dict(confusion)


# ------------------------------------------------------------
# Beispielaufruf
# ------------------------------------------------------------
if __name__ == "__main__":
    acc, conf = validate_bias_classifier(sample_size=1000, model="llama3.1", show_passes=False)
    print(f"Accuracy: {acc:.2%}  ({sum(conf.values())} Fälle)")
    print("Confusion‑Matrix (true → pred → count):")
    for (true, pred), cnt in sorted(conf.items()):
        print(f"  {true:28s} → {pred:28s}: {cnt}")
