import requests
import time
import subprocess


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


def query_ollama(model_name, prompt, gpu_nr=0, num_ctx=65536):
    """
    Sendet eine Anfrage an die Ollama API und gibt die Antwort zurück.
    """
    try:
        # API-Anfrage-Daten
        payload = {
            "model": model_name,
            "prompt": prompt,
            "stream": False,  # False bedeutet: vollständige Antwort wird zurückgegeben
            "options": {
                "temperature": 0,  # Setzt die Temperatur auf 0 für deterministische Antworten
                "seed": 0,
                "num_ctx": num_ctx,
                "max_tokens": 2048
            }
        }

        # POST-Anfrage an die API
        response = requests.post(OLLAMA_URLS[gpu_nr], json=payload)

        # Prüfe den Statuscode
        if response.status_code == 200:
            result = response.json()
            return result.get("response", "Keine Antwort erhalten.")
        else:
            return f"Fehler: {response.status_code} - {response.text}"

    except Exception as e:
        return f"Fehler beim Verbinden mit der Ollama API: {e}"

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