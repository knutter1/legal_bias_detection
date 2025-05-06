#!/usr/bin/env python3
import subprocess

def run_command(command):
    """
    Führt den übergebenen Shell-Befehl aus und gibt die Ausgabe in der Konsole aus.
    """
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print("Fehler:", stderr)
    except Exception as e:
        print("Fehler beim Ausführen des Befehls:", e)

def main():
    # Dictionary mit den Befehlen
    commands = {
        "1": {
            "name": "FLASK STARTEN",
            "command": "nohup /home/herzberg/project/venv/bin/python3.12 /home/herzberg/project/annotation_handler.py > /home/herzberg/project/logs/annotation_handler.logs 2>&1 &"
        },
        "2": {
            "name": "WATCHDOG FÜR FLASK STARTEN",
            "command": "nohup /home/herzberg/project/venv/bin/python3.12 /home/herzberg/project/watchdog.py > /home/herzberg/project/logs/watchdog.log 2>&1 &"
        },
        "3": {
            "name": "GPU AUSLASTUNG SEHEN",
            "command": "watch -n 1 \"nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits | awk -F, '{printf \\\"GPU %s: %s%% Load, %s/%s MiB VRAM\\n\\\", $1, $2, $3, $4}'\""
        },
        "4": {
            "name": "WIE VIELE USER GERADE AUF SERVER",
            "command": "who | awk '{print $1}' | sort | uniq -c"
        },
        "5": {
            "name": "OLLAMA STARTEN",
            "command": "nohup ~/ollama/bin/ollama serve > ~/ollama/ollama.log 2>&1 &"
        },
        "6": {
            "name": "MONGOD STARTEN",
            "command": "nohup ~/mongodb/bin/mongod --dbpath ~/mongodb-data --logpath ~/mongodb-logs/mongodb.log --port 27017 --bind_ip 127.0.0.1 --fork > ~/mongodb-logs/mongodb-nohup.log 2>&1 &"
        },
        "7": {
            "name": "ZÄHLE GESAMT BIASES IN DATENBANK",
            "command": "mongosh court_decisions --quiet --eval 'db.judgments.aggregate([{ $match: { selected_for_smaller_experiment: true, ollama_responses: { $exists: true, $ne: [] } } }, { $unwind: \"$ollama_responses\" }, { $match: { \"ollama_responses.run_id\": { $in: [4,5] } } }, { $project: { biasCount: { $size: { $ifNull: [ \"$ollama_responses.response.biases\", [] ] } } } }, { $group: { _id: null, total: { $sum: \"$biasCount\" } } }]).forEach(doc => print(doc.total))'"
        },
        "8": {
            "name": "GESAMTE ANZAHL AN ANNOTATIONS MIT ANNOTATOR AUSGEBEN LASSEN",
            "command": "mongosh court_decisions --quiet --eval 'db.judgments.aggregate([ { $match: { selected_for_smaller_experiment: true, ollama_responses: { $exists: true, $ne: [] } } }, { $unwind: \"$ollama_responses\" }, { $match: { \"ollama_responses.run_id\": { $in: [4,5] } } }, { $unwind: \"$ollama_responses.response.biases\" }, { $unwind: \"$ollama_responses.response.biases.annotations\" }, { $group: { _id: \"$ollama_responses.response.biases.annotations.annotator\", count: { $sum: 1 } } }, { $group: { _id: null, totalAnnotations: { $sum: \"$count\" }, annotators: { $push: { annotator: \"$_id\", count: \"$count\" } } } } ]).forEach(doc => print(\"Total annotations: \" + doc.totalAnnotations + \", Annotators: \" + JSON.stringify(doc.annotators)))'"
        },
        "9": {
            "name": "ALLE ANNOTATIONS AUSGEBEN LASSEN",
            "command": "mongosh court_decisions --quiet --eval 'db.judgments.aggregate([{ $match: { selected_for_smaller_experiment: true, ollama_responses: { $exists: true, $ne: [] } } }, { $unwind: \"$ollama_responses\" }, { $match: { \"ollama_responses.run_id\": { $in: [4,5] } } }, { $unwind: \"$ollama_responses.response.biases\" }, { $unwind: \"$ollama_responses.response.biases.annotations\" }, { $replaceRoot: { newRoot: \"$ollama_responses.response.biases.annotations\" } }]).forEach(doc => printjson(doc))'"
        },
        "10": {
            "name": "PRÜFE AUSLASTUNG VON GPU SPEICHER",
            "command": "[ $(nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits | awk -F',' '{used+=$1; total+=$2} END {print (used/total)>=0.9 ? 1 : 0}') -eq 1 ] && echo \"GPU-Speicher >= 90% ausgelastet\" || echo \"GPU-Speicher < 90% ausgelastet\""
        },
        "11": {
            "name": "FREIEN SPEICHER AUF FESTPLATTEN AUSGEBEN LASSEN",
            "command": "df -h"
        }
    }

    # Menü anzeigen
    print("Bitte wählen Sie einen Befehl aus:")
    for key, value in commands.items():
        print(f"{key}: {value['name']}")
    
    selection = input("Eingabe der Befehlsnummer: ").strip()
    if selection in commands:
        print(f"Ausführen von: {commands[selection]['name']}")
        run_command(commands[selection]['command'])
    else:
        print("Ungültige Auswahl.")

if __name__ == "__main__":
    main()
