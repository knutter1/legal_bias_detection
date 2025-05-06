import os
import time
import logging
from pymongo import MongoClient
from grab_text import get_clean_text_by_id
from ollama_essentials import query_ollama, is_gpu_memory_overloaded
import re
import uuid
from random import shuffle
from datetime import datetime


# Toggle for skipping already processed entries
SKIP_PROCESSED = True
RUN_ID = 5
TEST_ONLY = False
RELOAD_MODEL_IF_MEMORY_FULL = True


def connect_to_mongo():
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        db = client["court_decisions"]
        collection = db["judgments"]
        return collection
    except Exception as e:
        logging.error(f"Fehler bei der Verbindung zu MongoDB: {e}")
        raise

def create_temp_directory_safe():
    """Stellt sicher, dass der temporäre Ordner existiert."""
    temp_dir = os.path.join(os.getcwd(), "temp_files")
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        logging.info(f"Temp-Ordner erstellt: {temp_dir}")
    return temp_dir


def parse_bias_response(response):
    """
    Analysiert die Response eines LLMs auf Bias-Angaben und gibt eine strukturierte Darstellung zurück.

    :param response: Der Text der Modellantwort als String.
    :return: Eine strukturierte Darstellung der Bias-Angaben oder einen Fehlerhinweis.
    """
    # Wenn die Antwort "Kein Bias" enthält, das als einziges Ergebnis zurückgeben
    if response.strip() == "Kein Bias":
        return [{
            "id": str(uuid.uuid4()),
            "bias_type": "Kein Bias",
            "text_passage": None,
            "justification": None
        }]

    # Regex-Muster für die drei Teile der Antwort
    bias_pattern = r"- Identifizierter Bias: (.*?)\n- Textpassage: \"(.*?)\"\n- Begründung: (.*?)($|\nIdentifizierter Bias:)"

    # Suche nach Bias-Einträgen
    matches = re.finditer(bias_pattern, response, re.DOTALL)
    results = []

    for match in matches:
        bias_type = match.group(1).strip()
        text_passage = match.group(2).strip()
        justification = match.group(3).strip()

        results.append({
            "id": str(uuid.uuid4()),
            "bias_type": bias_type,
            "text_passage": text_passage,
            "justification": justification
        })

    # Überprüfen, ob Ergebnisse gefunden wurden
    if results:
        return results
    else:
        return [{
            "error": "Ungültige Response-Struktur"
        }]



def check_for_stop_flag(stop_file_path):
    with open(stop_file_path, 'r') as file:
        stop_flag = int(file.read().strip())  # Konvertiert den Inhalt in eine Ganzzahl (0 oder 1)
        if stop_flag:
            # Setze den Inhalt der Datei auf '0'
            with open(stop_file_path, 'w') as write_file:
                write_file.write('0')
            return True
        else:
            return False


def get_model_response(text, model, gpu_nr=0, num_ctx=8192):
    """
    Analysiert den Text und gibt die Antworten mit IDs zurück.
    """
    prompt_text = f"""
Du erhältst einen Urteilstext in deutscher Sprache. Deine Aufgabe ist es, diesen Text auf folgende Bias-Arten zu analysieren und eine präzise Begründung zu geben, warum die Textpassage einen Bias enthält. 
Verwende dazu die unten stehenden Definitionen und Unterscheidungskriterien:

1. Gender-Bias
Gender-Bias beschreibt die systematische, ungleiche Behandlung basierend auf dem Geschlecht.
Merkmale von Gender-Bias:
- Struktureller Bias: Verwendung von grammatikalischen Konstruktionen, die stereotype Annahmen fördern.
- Kontextueller Bias: Nutzung spezifischer Wörter oder Töne, die geschlechtsbezogene Rollen und Stereotypen verstärken.
- Stereotypisierung: Zuordnung von Eigenschaften oder Berufen basierend auf sozialen Geschlechterrollen.
Erkenne Gender-Bias durch:
- Analyse von Wortassoziationen (z. B. positive Adjektive für Frauen beziehen sich oft auf körperliche Eigenschaften).
- Überprüfung von Stereotypen bei beruflichen Begriffen.
- Analyse grammatikalischer Strukturen auf geschlechterbezogene Generalisierungen.

2. Religiöser Bias
Religiöser Bias bezieht sich auf implizite Einstellungen und Vorurteile, die häufig unterhalb der bewussten Wahrnehmung liegen und interreligiöse Kooperation behindern können.
Merkmale:
- Religiöser Bias kann auf sozialpsychologischen Unterschieden wie Status, Skripturen oder transnationalen Einflüssen basieren.
- Tendenzen zur Gruppenfavorisierung, bei der die Eigengruppe positiv und die Fremdgruppe negativ dargestellt wird.
- Unterschiedliche Bindungen an religiöse Schriften können die Wahrnehmung theologisch divergenter Gruppen verstärken.
Erkenne dies durch:
- Untersuchung impliziter Vorurteile.
- Unterschiede in der Reaktion auf interreligiöse Botschaften, basierend auf Inhalt und Quelle der Nachricht.
- Messung von Verzögerungen in der Zuordnung positiver oder negativer Attribute zu bestimmten religiösen Gruppen.

3. Rassistischer Bias
Rassistischer Bias kann implizit (relativ unbewusst) oder explizit (bewusst) sein. Sowohl implizite als auch explizite Vorurteile sind weit verbreitet und führen zu starken, negativen Konsequenzen.
Merkmale:
- Rassistischer Bias wird durch Kategorisierung, Stereotypisierung, Vorurteile und Diskriminierung verstärkt.
- Bias kann zu verzerrten Urteilen und diskriminierendem Verhalten führen, auch wenn die Absicht fehlt.
Erkenne dies durch:
- Die Präsenz von Stereotypen in sozialen oder beruflichen Interaktionen.
- Verzerrte Urteile oder Handlungen in Situationen mit Unsicherheit oder Zeitdruck.
- Unterschiede in der Behandlung von Individuen aufgrund von Gruppenmerkmalen.

4. Sexuelle Orientierung Bias 
Definition:
Der Bias bezüglich sexueller Orientierung umfasst die bewusste oder unbewusste Benachteiligung von Personen aufgrund ihrer sexuellen Präferenz. Dabei werden Entscheidungen oder Handlungen getroffen, die diese Personen schlechter stellen, sei es im Arbeitsumfeld, in Bildungseinrichtungen oder anderen gesellschaftlichen Kontexten.
Merkmale:
- Diskriminierung auf Basis von Stereotypen oder Vorurteilen gegenüber sexuellen Minderheiten.
- Ungleichbehandlung bei Beschäftigung, Beförderung oder anderen arbeitsbezogenen Entscheidungen.
- Verweigerung gleicher Rechte oder Dienstleistungen.
Erkenne dies durch:
- Vergleich der Behandlung von homosexuellen und heterosexuellen Personen unter ähnlichen Bedingungen.
- Analyse von Sprache und Handlungen, die implizit oder explizit Vorurteile widerspiegeln.
- Unverhältnismäßige Anwendung von Regeln, die bestimmte Gruppen benachteiligen.

5. Altersdiskriminierung
Altersdiskriminierung umfasst jede Form von ungleicher Behandlung oder Benachteiligung einer Person aufgrund ihres Alters, sofern diese nicht durch legitime sozialpolitische Ziele oder sachliche Gründe gerechtfertigt ist. Sie beinhaltet sowohl direkte als auch indirekte Diskriminierung.
Merkmale:
- Altersdiskriminierung unterscheidet sich durch die Anwendung spezifischer Altersgrenzen in sozialpolitischen Regelungen.
- Besonders relevant sind Verhältnismäßigkeitsprüfungen bei ungleichen Altersgrenzen.
Erkenne dies durch:
- Die Benachteiligung von Gruppen durch starr definierte Altersgrenzen, die keinen sachlichen Bezug zu den angestrebten Zielen haben.
- Fälle, bei denen Altersgrenzen spezifische Zugangsmöglichkeiten zu sozialpolitischen Leistungen blockieren.
- Indikatoren wie die Begrenzung der Arbeitslosenversicherung auf Altersgruppen.

6. Nationalität-Bias
Nationalitäts-Bias bezieht sich auf die systematische Verzerrung, bei der Länder oder deren Bevölkerung in einem ungenauen, stereotypischen oder abwertenden Licht dargestellt werden.
Merkmale:
- Stereotypische oder abwertende Sprache gegenüber bestimmten Nationalitäten.
- Themenfokus auf militärische Konflikte oder politische Instabilität für bestimmte Länder.
Erkenne dies durch:
- Themen wie Gewalt, Terrorismus oder Korruption, die in Bezug auf bestimmte Länder überrepräsentiert sind.

7. Behinderungen-Bias
Ein Bias gegenüber Menschen mit Behinderung bezieht sich auf automatisch aktivierte, unbewusste Einstellungen, die dazu führen, dass Menschen mit Behinderung negativ wahrgenommen oder behandelt werden. Diese Einstellungen basieren auf gesellschaftlichen Stereotypen und Assoziationen, die oft zu diskriminierendem Verhalten führen.
Merkmale:
- Negative implizite Präferenzen für nicht-behinderte Menschen gegenüber Menschen mit Behinderung.
- Automatische Assoziation von Behinderung mit negativen Begriffen (z.B. schlecht, inkompetent).
- Tendenz, Menschen mit Behinderung als kindlich oder weniger kompetent wahrzunehmen.
Erkenne dies durch:
- Analyse von Assoziationen zwischen Begriffen wie "Behinderung" und "negativ" im Text.
- Identifizierung von subtilen Formulierungen, die auf Mangel an Kompetenz oder Autonomie hindeuten.
- Untersuchen von impliziten Vorannahmen in Entscheidungsprozessen, die Menschen mit Behinderung benachteiligen.


8. Erscheinung-Bias
Körperliche Attraktivität ist eine Heuristik, die oft als Indikator für wünschenswerte Eigenschaften verwendet wird. Studien zeigen, dass Menschen attraktiven Individuen eher moralische Eigenschaften zuschreiben als unattraktiven, ein Effekt, der stärker ist als die Tendenz, attraktive Personen mit positiven nicht-moralischen Eigenschaften zu verbinden. Dies deutet darauf hin, dass physische Attraktivität Wahrnehmungen von moralischem Charakter besonders stark beeinflusst.
Merkmale:
- Attraktive Personen werden eher als moralisch wahrgenommen.
- Die Zuschreibung moralischer Eigenschaften ist stärker als die Zuschreibung nicht-moralischer Eigenschaften.
- Die Wahrnehmung moralischer Eigenschaften basiert auf schnellen heuristischen Einschätzungen.
Erkenne dies durch:
- Überprüfung, ob moralische Eigenschaften wie Ehrlichkeit oder Vertrauenswürdigkeit mit physischer Attraktivität verknüpft werden.
- Analyse von Bewertungen, die soziale Erwünschtheit überproportional auf attraktive Personen projizieren.
- Vergleich moralischer und nicht-moralischer Attributionsmuster für attraktive vs. unattraktive Personen.


9. Bias durch sozioökonomischen Status: 
Bias aufgrund der sozioökonomischen Stellung beschreibt systematische Verzerrungen, die darauf beruhen, dass Menschen aufgrund ihrer wirtschaftlichen und sozialen Position ungleich behandelt oder beurteilt werden, was die Chancengleichheit beeinträchtigt.
Merkmale
- Verzerrungen im Zugang zu Bildung und Arbeitsmöglichkeiten.
- Unterschiedliche Wahrnehmung und Behandlung basierend auf Einkommen oder Vermögen.
- Einfluss der subjektiven Wahrnehmung von Status auf Entscheidungen.
Erkenne dies durch
- Analyse von Diskrepanzen zwischen sozioökonomischen Gruppen in Bildung, Einkommen oder politischer Partizipation.
- Identifizierung von ungleichen Ergebnissen trotz vergleichbarer Fähigkeiten oder Ressourcen.
- Beobachtung sozialer Mobilität und struktureller Barrieren.


Hier ist der Urteilstext: 

{text}

ENDE DES URTEILSTEXTES
Ausgabeformat:
Wenn das Urteil keinen der genannten Biases enthält, antwortest du nur mit den Worten “Kein Bias” und begründest deine Entscheidung nicht. Sonst antwortest du wie folgt:
Wenn du eine Art der Biases gefunden hast, gliederst du deine Antwort in ein Format mit 3 eindeutigen Teilen, damit ich es maschinell weiter verarbeiten kann.
Präsentiere deine Antwort in Fließtext und in folgendem Format:
Identifizierter Bias: [Hier nennst du die Art des Bias, also eines aus "Gender-Bias", "Religiöser Bias", "Rassistischer Bias", "Sexuelle Orientierung Bias", "Altersdiskriminierung", "Nationalität-Bias", "Behinderungen-Bias", "Erscheinung-Bias", "Bias durch sozioökonomischen Status"] 
Textpassage: "[Hier zitierst du die relevante Passage aus dem Urteilstext]"
Begründung: [Begründe, warum diese Passage diesen Bias zeigt]
Wenn der Urteilstext mehrfach Biases beinhaltet, antwortest du mehrfach in diesem Format.  
        """

    start_time = time.time()
    try:
        response = query_ollama(model, prompt_text, gpu_nr=gpu_nr, num_ctx=num_ctx)
    except Exception as e:
        logging.error(f"Fehler bei der Modellabfrage: {e}")
        return []

    elapsed_time = round(time.time() - start_time, 2)

    # Bias-Response analysieren
    parsed_biases = parse_bias_response(response)

    return {
        "id": str(uuid.uuid4()),
        "model": model,
        "response": response,
        #"parsed_biases": parsed_biases,  # Strukturierte Darstellung
        "time_taken": elapsed_time,
        "timestamp": time.time(),
        "run_id": RUN_ID
    }


def bias_check_single(model, run_id):
    """
    Hauptfunktion für die Biasüberprüfung.
    """
    # Verbindung zur MongoDB
    collection = connect_to_mongo()

    # Filter: Elemente mit "selected_for_experiment" = true
    query = {"selected_for_smaller_experiment": True}
    if SKIP_PROCESSED:
        query["ollama_responses.run_id"] = {"$ne": run_id}

    # Elemente abrufen und nach num_characters sortieren
    elements = list(collection.find(query))
    elements.sort(key=lambda x: x["num_characters"])

    # Aufteilen in Teilarrays nach Kontextgrößen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (element["num_characters"] + 5000) // 3     # +1500, weil Promptgerüst ca. 1500 token
        for ctx in context_thresholds:
            if num_chars <= ctx:
                context_buckets[ctx].append(element)
                break

    # Ausgabe der Anzahl der Elemente in jedem Kontextbucket
    for ctx, bucket in context_buckets.items():
        print(f"Kontextgröße {ctx}: {len(bucket)} Elemente")

    # Verarbeitung der Buckets
    for ctx, bucket in context_buckets.items():
        print(f"Beginne Verarbeitung für Kontextgröße {ctx} mit {len(bucket)} Elementen.")
        # Elemente durchmischen, damit Zeit besser abgeschätzt werden kann
        shuffle(bucket)
        for element in bucket:

            # Lesen der 'stop.md'-Datei und Konvertieren des Inhalts in eine Variable
            if check_for_stop_flag(stop_file_path="/home/herzberg/project/stop.md"):
                return

            # Text abrufen
            text = get_clean_text_by_id(element["_id"])

            processed_count = collection.count_documents(
                {"ollama_responses.run_id": RUN_ID, "selected_for_smaller_experiment": True})

            # Modellantwort abrufen, prüft alle 10 Abfragen ob Speicherprobleme sind, lädt dann model neu
            if RELOAD_MODEL_IF_MEMORY_FULL and processed_count % 10 == 0 and is_gpu_memory_overloaded(threshold=.9):
                _ctx = 131072
                print("Lade model neu, weil GPU-Speicher voll ist")
            else:
                _ctx = ctx

            # get response from model and measure time taken
            start_time = time.time()
            response_list = get_model_response(model=model, text=text, num_ctx=_ctx)
            end_time = time.time()

            if TEST_ONLY:
                # Nur Ausgabe der Antwort
                print(f"Response for element: {response_list}")
            else:
                # Speichern der gesamten response_list
                collection.update_one(
                    {"_id": element["_id"]},
                    {"$push": {"ollama_responses": {"$each": [response_list]}}}  # Wrap response_list in a list
                )

            # Ausgabe wie viele Elemente in Datenbank bereits gespeichert wurden
            print(
                f"{datetime.now().strftime("%H:%M:%S")} - Verarbeitete Elemente für Run {RUN_ID}: {processed_count} in {round(end_time - start_time, 2)}s")

        print(f"Verarbeitung für Kontextgröße {ctx} abgeschlossen.")



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("process_and_store.log"),
            logging.StreamHandler()
        ]
    )

    bias_check_single(model="deepseek-r1:70b", run_id=RUN_ID)
