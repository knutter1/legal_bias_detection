import os
import re
import shutil
import time

import requests
from pymongo import MongoClient
import fitz  # PyMuPDF
from bs4 import BeautifulSoup

def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")  # Default MongoDB port
    db = client["court_decisions"]  # Database name
    collection = db["judgments"]  # Collection name
    # print("MongoDB connected and database/collection created.")
    return collection


def clean_text(text):
    """
    Bereinigt den eingegebenen Text, indem NBSP-Zeichen, unnötige
    Zeilenumbrüche und überflüssige Leerzeichen entfernt werden.
    """
    # 1. Ersetzen von NBSP-Zeichen durch reguläre Leerzeichen.
    text = text.replace('\xa0', '')

    # 2. Ersetzen von Zeilenumbrüchen, denen kein Satzzeichen (., ?, !) vorausgeht,
    #    durch ein Leerzeichen. Dies entfernt unnötige Umbrüche innerhalb eines Satzes.
    text = re.sub(r'(?<!\w\.)\n(?!\n)', ' ', text)
    # Erklärung des Regex:
    # (?<!\w\.) : Negative Lookbehind. Stellt sicher, dass das Zeichen vor dem \n kein
    #             Buchstabe oder eine Ziffer gefolgt von einem Punkt ist.
    # \n        : Matcht einen Zeilenumbruch.
    # (?!\n)    : Negative Lookahead. Stellt sicher, dass der Zeilenumbruch nicht
    #             von einem weiteren Zeilenumbruch gefolgt wird.

    # 3. Entfernen des Musters "Seite <Zahl>".
    text = re.sub(r"Seite \d+", "", text)

    # 4. Ersetzen von mehreren Leerzeichen (inkl. Zeilenumbrüchen, die nicht zu Absätzen gehören)
    #    durch ein einziges Leerzeichen.
    text = re.sub(r'\s{2,}', ' ', text).strip()

    return text


def download_file(url, temp_dir, max_retries=10):
    """
    Lädt eine Datei von einer URL herunter und speichert sie im temporären Ordner.
    Versucht es bis zu max_retries Mal, falls ein Fehler auftritt.
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Versuch {attempt}: Lade Datei von {url} herunter...")
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Überprüft, ob der HTTP-Statuscode erfolgreich ist

            file_name = url.split("/")[-1]  # Dateiname aus der URL extrahieren
            file_path = os.path.join(temp_dir, file_name)

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"Datei erfolgreich heruntergeladen: {file_path}")
            return file_path  # Erfolgreicher Download, Rückgabe des Pfads

        except Exception as e:
            print(f"Fehler beim Herunterladen der Datei (Versuch {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(2)  # Optionale Wartezeit zwischen den Versuchen
            else:
                print("Maximale Anzahl von Versuchen erreicht. Download fehlgeschlagen.")
                return None


def extract_text_from_html(html_file):
    """Extrahiert Text aus einer HTML-Datei."""
    with open(html_file, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")
        for header_footer in soup.find_all(["header", "footer"]):
            header_footer.decompose()
        return soup.get_text(separator="\n")

def extract_text_from_pdf(pdf_file):
    """Extrahiert Text aus einer PDF-Datei."""
    try:
        doc = fitz.open(pdf_file)
        text = ""
        for page in doc:
            text += page.get_text("text")
        return text
    except Exception as e:
        print(f"Fehler beim PDF-Parsing: {e}")
        return ""

def create_temp_directory():
    """
    Erstellt einen temporären Ordner für Dateien.
    """
    temp_dir = "temp_files"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    return temp_dir

def clean_temp_directory(temp_dir):
    """
    Löscht den temporären Ordner und alle darin enthaltenen Dateien.
    """
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

def get_clean_text_by_id_online(entry_id):
    """
    Nimmt die ID eines Eintrags in MongoDB und gibt den bereinigten Text zurück.
    """
    collection = connect_to_mongo()
    entry = collection.find_one({"_id": entry_id})

    if not entry:
        print(f"Kein Eintrag mit ID {entry_id} gefunden.")
        return None

    temp_dir = create_temp_directory()
    text_content = None
    downloaded_file = None

    try:
        # Priorisiere HTML-Dateien
        if "HTML" in entry and "Datei" in entry["HTML"]:
            url = "https://entscheidsuche.ch/docs/" + entry["HTML"]["Datei"]
            downloaded_file = download_file(url, temp_dir)
            if downloaded_file:
                text_content = extract_text_from_html(downloaded_file)
        elif "PDF" in entry and "Datei" in entry["PDF"]:
            url = "https://entscheidsuche.ch/docs/" + entry["PDF"]["Datei"]
            downloaded_file = download_file(url, temp_dir)
            if downloaded_file:
                text_content = extract_text_from_pdf(downloaded_file)

        # Bereinige Text
        if text_content:
            return clean_text(text_content)
        else:
            print(f"Keine Inhalte für die angegebene ID {entry_id} verfügbar.")
            return None
    finally:
        # Lösche temporäre Dateien
        if downloaded_file and os.path.exists(downloaded_file):
            os.remove(downloaded_file)

def get_clean_text_by_id(entry_id):
    """
    Nimmt die ID eines Eintrags in MongoDB und gibt den bereinigten Text zurück.
    """
    collection = connect_to_mongo()
    entry = collection.find_one({"_id": entry_id})

    if not entry:
        print(f"Kein Eintrag mit ID {entry_id} gefunden.")
        return None

    textfile_path = os.path.expanduser("~/project/text_files/" + entry.get("abbreviation") + ".txt")

    try:
        # Datei öffnen und lesen
        with open(textfile_path, "r", encoding="utf-8") as file:
            content = file.read()
        return clean_text(content)

    except FileNotFoundError:
        print(f"Die Datei {textfile_path} wurde nicht gefunden.")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")



def main():
    temp_dir = create_temp_directory()
    collection = connect_to_mongo()
    judgments = collection.find()

    for judgment in judgments:
        filename = None
        text_content = None
        downloaded_file = None  # Temporäre Datei

        # Priorisiere HTML-Dateien
        if "HTML" in judgment and "Datei" in judgment["HTML"]:
            filename = "https://entscheidsuche.ch/docs/" + judgment["HTML"]["Datei"]
            downloaded_file = download_file(filename, temp_dir)
            if downloaded_file:
                text_content = extract_text_from_html(downloaded_file)
        elif "PDF" in judgment and "Datei" in judgment["PDF"]:
            filename = "https://entscheidsuche.ch/docs/" + judgment["PDF"]["Datei"]
            downloaded_file = download_file(filename, temp_dir)
            if downloaded_file:
                text_content = extract_text_from_pdf(downloaded_file)

        # Bereinige Text und zeige Vorschau an
        if text_content:
            cleaned_text = clean_text(text_content)
        # Lösche die temporäre Datei
        if downloaded_file and os.path.exists(downloaded_file):
            os.remove(downloaded_file)

        break  # Entfernen, um alle Dokumente zu verarbeiten

    # Lösche den gesamten temporären Ordner
    # clean_temp_directory(temp_dir)

if __name__ == "__main__":
    main()
