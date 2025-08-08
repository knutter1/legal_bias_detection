"""insert_korean_judgments.py
Insert Korean court judgments from an Excel file into MongoDB – zero configuration.

Run it with nothing but:

    $ python insert_korean_judgments.py       # writes to DB

Set the optional environment variable TEST_ONLY to perform a dry‑run:

    $ TEST_ONLY=true python insert_korean_judgments.py

All file names and Mongo‑Defaults correspond to the requirements stated in the
initial prompt and the patterns used in the other *insert_* scripts.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd
from pymongo import MongoClient
from bson import ObjectId  # noqa: F401 – ObjectId type hints for completeness

# ---------------------------------------------------------------------------
# Configuration -------------------------------------------------------------
# ---------------------------------------------------------------------------

DEFAULT_TEST_ONLY = os.getenv("TEST_ONLY", "false").lower() in {"1", "true", "yes"}
DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DEFAULT_DB_NAME = os.getenv("MONGO_DB_NAME", "court_decisions")

EXCEL_FILE = "Korean_judgments_2005_2025.xlsx"
PROMPT_SUMMARY_FILE = "prompt_summary_korean.txt"
PROMPT_BIAS_FILE = "prompt_bias_detection_korean.txt"
COLLECTION_NAME = "judgments"

# ---------------------------------------------------------------------------
# Helper functions ----------------------------------------------------------
# ---------------------------------------------------------------------------

def load_prompt(path: str) -> str:
    """Return file contents without BOM and trailing whitespace."""
    with open(path, "r", encoding="utf-8-sig") as fp:
        return fp.read().rstrip()


def get_next_id(collection) -> int:
    """Return the next free integer id (max(id) + 1)."""
    doc = collection.find_one(sort=[("id", -1)], projection={"id": 1})
    return (doc["id"] + 1) if doc and "id" in doc else 1


def as_clean_dict(row: pd.Series) -> Dict[str, Any]:
    """Convert a pandas row to dict with NaN → None."""
    return {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

# ---------------------------------------------------------------------------
# Core class ----------------------------------------------------------------
# ---------------------------------------------------------------------------

class KoreanJudgmentInserter:
    """Insert (or dry‑run) Korean judgments from Excel into MongoDB."""

    def __init__(
        self,
        excel_file: str = EXCEL_FILE,
        prompt_summary_file: str = PROMPT_SUMMARY_FILE,
        prompt_bias_file: str = PROMPT_BIAS_FILE,
        mongo_uri: str = DEFAULT_MONGO_URI,
        db_name: str = DEFAULT_DB_NAME,
        test_only: bool = DEFAULT_TEST_ONLY,
    ) -> None:
        self.excel_file = excel_file
        self.prompt_summary = load_prompt(prompt_summary_file)
        self.prompt_bias = load_prompt(prompt_bias_file)
        self.test_only = test_only

        # Mongo connection (created only if needed)
        self.client = MongoClient(mongo_uri)
        self.collection = self.client[db_name][COLLECTION_NAME]

    # --------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------

    def run(self) -> None:
        df: pd.DataFrame = pd.read_excel(self.excel_file)
        next_id: int = get_next_id(self.collection) if not self.test_only else 0

        inserts: List[Dict[str, Any]] = []
        for idx, row in df.iterrows():
            raw: Dict[str, Any] = as_clean_dict(row)
            origin_url: str | None = raw.get("origin_url")

            # Skip duplicates (live mode only)
            if (
                not self.test_only
                and origin_url
                and self.collection.count_documents({"origin_url": origin_url}) > 0
            ):
                print(f"[SKIP] origin_url already exists → {origin_url}")
                continue

            # Build doc -------------------------------------------------
            doc: Dict[str, Any] = raw.copy()
            doc["full_text"] = doc.pop("origin_text", None)  # rename field
            doc.update(
                {
                    "language": "Korean",
                    "prompt_summary": self.prompt_summary,
                    "prompt_bias_detection": self.prompt_bias,
                }
            )

            # id handling (increment only if LIVE)
            if self.test_only:
                doc_id = idx + 1  # deterministic but not persisted
            else:
                next_id += 1
                doc_id = next_id
            doc["id"] = doc_id

            inserts.append(doc)
            prefix = "[TEST]" if self.test_only else "[QUEUE]"
            print(f"{prefix} Prepared doc id={doc_id} origin_url={origin_url}")

        # Write to DB -------------------------------------------------
        if self.test_only:
            print(f"[TEST] Dry‑run complete. {len(inserts)} documents would be inserted.")
            return

        if not inserts:
            print("[INFO] No new documents to insert – up to date.")
            return

        result = self.collection.insert_many(inserts)
        print(f"[OK] Inserted {len(result.inserted_ids)} document(s) into MongoDB.")

    def mark_korean_selected(self, flag: bool = True) -> int:
            """Set `selected_for_annotation` on all docs with language="Korean".
            Returns number of modified documents (0 in TEST mode)."""
            query = {"language": "Korean"}
            update = {"$set": {"selected_for_annotation": flag}}
            if self.test_only:
                count = self.collection.count_documents(query)
                print(f"[TEST] Would set selected_for_annotation={flag} on {count} Korean docs.")
                return 0
            result = self.collection.update_many(query, update)
            print(f"[OK] Updated {result.modified_count}/{result.matched_count} Korean docs (selected_for_annotation={flag}).")
            return result.modified_count

# ---------------------------------------------------------------------------
# main ----------------------------------------------------------------------
# ---------------------------------------------------------------------------


def main() -> None:
    inserter = KoreanJudgmentInserter()
    mode = "TEST-ONLY" if inserter.test_only else "LIVE"
    print(f"[INIT] Marking Korean docs selected_for_annotation=true in {mode} mode …")
    inserter.mark_korean_selected(True)


if __name__ == "__main__":
    main()
