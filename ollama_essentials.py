import requests
import time
import subprocess

import json
import requests
import time
from requests.exceptions import Timeout, ConnectionError, HTTPError


# Ollama API-Endpunkt
OLLAMA_URLS = ["http://localhost:11434/api/generate", "http://localhost:11435/api/generate"]

MODELS = [
    "llama3.3",
    "llama3.2",
    "alibayram/erurollm-9b-instruct"
]

def is_gpu_memory_overloaded(threshold=0.9):
    """
    Prüft, ob der gesamte GPU-Speicher auf einem Server zu mindestens 90 % ausgelastet ist.

    Args:
        threshold (float): Auslastungsschwelle (Standard: 0.9, d. h. 90 %).

    Returns:
        bool: True, wenn der GPU-Speicher insgesamt >= threshold ausgelastet ist, sonst False.
    """
    try:
        # Hole die Ausgabe von `nvidia-smi` im Format der GPU-Speicherstatistiken
        result = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=memory.used,memory.total', '--format=csv,nounits,noheader'], encoding='utf-8')

        # Parse die Ausgabe und berechne die Gesamtauslastung
        total_used = 0
        total_memory = 0
        for line in result.strip().split('\n'):
            used, total = map(int, line.split(','))
            total_used += used
            total_memory += total

        # Berechne die Gesamtauslastung
        utilization = total_used / total_memory
        return utilization >= threshold

    except Exception as e:
        print(f"Fehler bei der GPU-Speicherabfrage: {e}")
        return False


def query_ollama(
        model_name: str,
        prompt: str,
        gpu_nr: int = 0,
        num_ctx: int = 65_536,
        max_retries: int = 5,
        backoff_base: int = 2,
):
    """
    Fragt die Ollama-API bis zu `max_retries`-mal ab.
    Bricht vorher ab, sobald eine gültige Antwort vorliegt.
    """

    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "seed": 0,
            "num_ctx": num_ctx,
            "max_tokens": 2048,
        },
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                OLLAMA_URLS[gpu_nr],
                json=payload,
                timeout=(10, 1200),        # (connect, read) Timeouts
            )
            resp.raise_for_status()        # wirft HTTPError bei 4xx/5xx

            data = resp.json()             # wirft JSONDecodeError bei Ungültigem
            answer = data.get("response", "")
            if answer:                     # gültige, nicht-leere Antwort
                return answer
            raise ValueError("Leere Antwort erhalten")

        except (Timeout, ConnectionError, HTTPError,
                ValueError, json.JSONDecodeError) as err:

            if attempt == max_retries:
                return f"Fehler nach {max_retries} Versuchen: {err}"

            wait = backoff_base ** (attempt - 1)   # 1 s, 2 s, 4 s, 8 s …
            print(f"[{model_name}] Versuch {attempt}/{max_retries} fehlgeschlagen "
                  f"({err}). Neuer Versuch in {wait}s …")
            time.sleep(wait)


def ask(model_name, prompt):
    # print(f"{model_name}:")

    # Anfrage an Ollama senden
    response = query_ollama(model_name, prompt)

    # Antwort anzeigen
    # print("Antwort des Modells:")
    # print(response + "\n")
    return response

def test_all_models(prompt = "Was ist die Hauptstadt von Deutschland?"):
    print(f"Prompt: {prompt}\n")
    for model_name in MODELS:
        start_time = time.time()
        # Modellname und Eingabeaufforderung (Prompt)
        print(f"{model_name}:\n{ask(model_name, prompt)}\n"
              f"response took {round(time.time() - start_time, 2)} seconds\n"
              f"------------------------------------------------------------")


if __name__ == "__main__":
    start_time = time.time()
    response = query_ollama("llama3.3", "Schreibe eine Kurzgeschichte über Gerichtsurteile aus der Schweiz.")
    print(f"Took {time.time() - start_time}s for this response:\n{response}")