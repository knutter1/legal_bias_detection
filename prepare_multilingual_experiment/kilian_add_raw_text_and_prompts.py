import json
import logging
from bson import ObjectId
from pymongo import MongoClient
from tqdm import tqdm

# Import der benötigten Funktion aus der bereitgestellten Datei grab_text.py
# Stellen Sie sicher, dass grab_text.py im selben Verzeichnis oder im Python-Pfad liegt.
from grab_text import get_clean_text_by_id_online

# Logging-Konfiguration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definition der Prompts, extrahiert aus den bereitgestellten Skripten
PROMPT_BIAS_DETECTION = """
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

PROMPT_SUMMARY = """SYSTEM: Du bist ein juristischer Assistent, der Schweizer Gerichtsurteile neutral und faktenorientiert zusammenfasst.
Extrahiere ausschließlich explizit genannte Informationen aus dem Text. Fehlende Angaben werden weggelassen.

Fasse das Urteil Schritt für Schritt zusammen:

1. Parteien:
   - Kläger/Antragsteller (z. B. IV-Stelle, Versicherungsnehmer) + Beruf (falls genannt)
   - Beklagter (z. B. Disziplinarbeklagter, Versicherungsgesellschaft) + Beruf (falls genannt)
   - Richter: Name und Hintergründe (z. B. "Richterin A. Meyer, Spezialgebiet Verwaltungsrecht")

2. Gericht & Verfahren:
   - Gericht (z. B. Obergericht des Kantons Bern)
   - Anzahl und Art der Vorinstanzen (z. B. 1. Instanz Sozialgericht, 2. Instanz Obergericht)
   - Entscheidungsdatum
   - Rechtsmittel: Wurde Berufung eingelegt? (Ja/Nein)
   - Rechtskräftigkeit: (Ja/Nein)

3. Sachverhalt:
   - Art des Vorfalls (z. B. Unfall, Pflichtverletzung)
   - Medizinische Details: Verletzungsart, Behandlung (z. B. Muskelruptur, neurologische Folgen)
   - Erwerbsstatus vor/nach dem Vorfall (z. B. "angestellt" → "100 % invalid")

4. Rechtliches:
   - Relevante Gesetze/Regelungen (z. B. Art. 12 BGFA, Versicherungsrecht) + Anzahl der zitierten Gesetze
   - Anzahl der zitierten medizinischen Gutachten/Berichte
   - Verwendete Argumentationsquellen (z. B. Bundesgerichtsurteile, Gesetzeskommentare)

5. Entscheidung:
   - Entscheidungsgründe (max. 3 Kernargumente)
   - Tenor: Höhe der Strafe/Entschädigung (z. B. CHF 1‘500), verhängte Strafen (z. B. Berufsausübungsverbot)

Format:
- Parteien: [Kläger] ([Beruf]), [Beklagter] ([Beruf]), [Richter] ([Hintergrund])
- Gericht: [Name], [Vorinstanzen], [Entscheidungsdatum], [Rechtsmittel: Ja/Nein], [Rechtskräftigkeit: Ja/Nein]
- Sachverhalt: [Vorfall], [Medizinische Details], [Erwerbsstatus vor/nach]
- Rechtliches: [Gesetze] ([Anzahl]), [Gutachten] ([Anzahl]), [Argumentationsquellen]
- Entscheidung: [Gründe], [Tenor]


Hier ist der Urteilstext: 
{text}
"""


class TextAndPromptUpdater:
    """
    Eine Klasse zum Aktualisieren von Gerichtsurteilen in MongoDB mit Rohtext und vordefinierten Prompts.
    """

    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="court_decisions"):
        """
        Initialisiert den Updater und verbindet sich mit der MongoDB.

        Args:
            mongo_uri (str): Die Verbindungs-URI für MongoDB.
            db_name (str): Der Name der Datenbank.
        """
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            self.collection = self.db["judgments"]
            # Überprüfen der Verbindung
            self.client.admin.command('ping')
            logging.info(f"Erfolgreich mit MongoDB verbunden. Datenbank: '{db_name}'")
        except Exception as e:
            logging.error(f"Fehler bei der Verbindung zu MongoDB: {e}")
            raise

    def update_documents_with_text_and_prompts(self):
        """
        Fügt den Rohtext und die Prompt-Texte zu den ausgewählten Dokumenten hinzu.

        Die Methode durchläuft alle Dokumente, die die Kriterien
        'selected_for_smaller_experiment: true' und 'language: de' erfüllen,
        holt den dazugehörigen Text und fügt ihn sowie die Prompts hinzu.
        """
        query = {
            "selected_for_smaller_experiment": True,
            "language": "de"
        }

        documents_to_update = list(self.collection.find(query))
        if not documents_to_update:
            logging.warning("Keine Dokumente gefunden, die die Kriterien erfüllen. Es wird nichts aktualisiert.")
            return

        logging.info(f"{len(documents_to_update)} Dokumente zum Aktualisieren gefunden.")

        for doc in tqdm(documents_to_update, desc="Aktualisiere Dokumente"):
            try:
                doc_id = doc["_id"]

                # 1. Rohtext mit der Online-Funktion abrufen
                clean_text = get_clean_text_by_id_online(doc_id)

                if not clean_text:
                    logging.warning(f"Konnte für Dokument {doc_id} keinen Text abrufen. Überspringe.")
                    continue

                # 2. Update-Payload erstellen
                update_payload = {
                    "$set": {
                        "full_text": clean_text,
                        "prompt_bias_detection": PROMPT_BIAS_DETECTION,
                        "prompt_summary": PROMPT_SUMMARY
                    }
                }

                # 3. Dokument in der Datenbank aktualisieren
                self.collection.update_one({"_id": doc_id}, update_payload)

            except Exception as e:
                logging.error(f"Fehler bei der Verarbeitung von Dokument {doc.get('_id', 'N/A')}: {e}")

        logging.info("Aktualisierung der Dokumente abgeschlossen.")

    def test_and_save_first_element(self, output_filename="test_element_raw_text.json"):
        """
        Holt das erste aktualisierte Dokument aus der DB, gibt es aus und speichert es als JSON.

        Args:
            output_filename (str): Der Dateiname für die JSON-Ausgabedatei.
        """

        # Hilfsfunktion zur JSON-Serialisierung von ObjectId
        def json_default(o):
            if isinstance(o, ObjectId):
                return str(o)
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

        try:
            # Finde ein Dokument, das das neue Feld 'full_text' enthält
            test_element = self.collection.find_one({"full_text": {"$exists": True}})

            if not test_element:
                logging.warning("Kein aktualisiertes Dokument zum Testen gefunden.")
                return

            logging.info(f"Test-Element gefunden: {test_element['_id']}")
            print("\n--- Vollständiges Test-Element ---")
            # Konvertiert das BSON-Dokument in einen String für eine saubere Ausgabe
            print(json.dumps(test_element, indent=2, default=json_default, ensure_ascii=False))
            print("---------------------------------\n")

            # Speichere das Dokument in einer JSON-Datei
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(test_element, f, indent=4, default=json_default, ensure_ascii=False)

            logging.info(f"Das Test-Element wurde erfolgreich in '{output_filename}' gespeichert.")

        except Exception as e:
            logging.error(f"Fehler beim Testen und Speichern des Elements: {e}")


if __name__ == "__main__":
    # Hauptausführungsblock
    updater = TextAndPromptUpdater()

    # 1. Hauptfunktion zur Aktualisierung der Dokumente aufrufen
    updater.update_documents_with_text_and_prompts()

    # 2. Testfunktion aufrufen, um das Ergebnis zu überprüfen
    updater.test_and_save_first_element()