#!/home/herzberg/project/venv/bin/python3.12
import subprocess
import time
from datetime import datetime
import sys
import os

# Befehl, der alle Prozesse des Benutzers 'herzberg' sucht, deren Kommandozeile 'annotation_handler.py' enthält.
CHECK_CMD = "pgrep -u herzberg -f annotation_handler.py"

# Befehl zum Starten des annotation_handler.py über nohup.
START_CMD = ("nohup /home/herzberg/project/venv/bin/python3.12 "
             "/home/herzberg/project/annotation_handler.py "
             "> /home/herzberg/project/logs/annotation_handler.logs 2>&1 &")

def log(message: str):
    """Schreibt eine Logzeile mit Zeitstempel."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def is_annotation_handler_running() -> bool:
    """
    Prüft, ob annotation_handler.py läuft, indem versucht wird, eine gültige PID zu extrahieren.
    Wenn keine gültige PID vorhanden ist, wird False zurückgegeben.
    """
    result = subprocess.run(CHECK_CMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout_str = result.stdout.strip()
    # Zerlege die Ausgabe in Zeilen/Einzelelemente (über whitespace getrennt)
    pid_lines = stdout_str.split()
    # Filtere alle Einträge, die ausschließlich Ziffern enthalten und entferne die eigene PID.
    valid_pids = [pid for pid in pid_lines if pid.isdigit()]
    
    # mindestens 2 Prozesse von FLask, eher 3
    return len(valid_pids) > 1

def start_annotation_handler():
    """Startet annotation_handler.py im Hintergrund."""
    subprocess.Popen(START_CMD, shell=True)
    log("annotation_handler.py wurde gestartet.")

if __name__ == "__main__":
    while True:
        if not is_annotation_handler_running():
            log("annotation_handler.py läuft nicht. Starte neu …")
            start_annotation_handler()
        
        time.sleep(60)  # Überprüft alle 60 Sekunden
