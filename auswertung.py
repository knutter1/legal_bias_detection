import csv
import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
from sklearn.metrics import cohen_kappa_score
import time

class AnnotationComparer:
    def __init__(self,
                 mongo_uri='mongodb://localhost:27017/',
                 db_name='court_decisions',
                 collection_name='judgments',
                 run_ids=(4, 5),
                 output_csv='annotation_comparison.csv'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.run_ids = run_ids
        self.output_csv = output_csv
        self.mapping = {}  # bias_id -> {'Sabine': {...}, 'Tom': {...}}

    def load_annotations(self):
        """Lade alle relevanten Annotationen für Sabine und Tom."""
        pipeline = [
            {'$match': {
                'selected_for_smaller_experiment': True,
                'ollama_responses.run_id': {'$in': list(self.run_ids)}
            }},
            {'$unwind': '$ollama_responses'},
            {'$match': {'ollama_responses.run_id': {'$in': list(self.run_ids)}}},
            {'$unwind': '$ollama_responses.response.biases'},
            {'$unwind': '$ollama_responses.response.biases.annotations'},
            {'$project': {
                'bias_id': '$ollama_responses.response.biases.id',
                'bias_type_id': '$ollama_responses.response.biases.annotations.bias_type_id',
                'annotator': '$ollama_responses.response.biases.annotations.annotator',
                'comment': '$ollama_responses.response.biases.annotations.comment'
            }}
        ]
        for doc in self.collection.aggregate(pipeline, allowDiskUse=True):
            bid = doc['bias_id']
            ann = {
                'bias_type_id': doc.get('bias_type_id', 0),
                'comment': doc.get('comment', '')
            }
            if bid not in self.mapping:
                self.mapping[bid] = {'Sabine Wehnert': None, 'Tom Herzberg': None}
            self.mapping[bid][doc['annotator']] = ann

    def build_dataframes(self):
        """
        Erzeuge die verschiedenen DataFrames:
        1) Alle Annotationen
        2) ohne beide type_id==0
        3) ohne beide 0 und ohne Sabine type_id==10
        4) Häufigkeiten pro annotator
        5) Kreuztabelle Sabine vs Tom
        """
        rows = []
        for bias_id, anns in sorted(self.mapping.items()):
            sab = anns.get('Sabine Wehnert') or {'bias_type_id': None, 'comment': ''}
            tom = anns.get('Tom Herzberg') or {'bias_type_id': None}
            rows.append({
                'bias_id': bias_id,
                'sabine_type': sab['bias_type_id'],
                'tom_type': tom['bias_type_id'],
                'sabine_comment': sab.get('comment', '')
            })
        df_all = pd.DataFrame(rows)

        df_no_both_zero = df_all[~((df_all['sabine_type'] == 0) & (df_all['tom_type'] == 0))]
        df_no_zero_ten = df_no_both_zero[~(df_no_both_zero['sabine_type'] == 10)]

        freq_sabine = df_all['sabine_type'].value_counts().sort_index()
        freq_tom = df_all['tom_type'].value_counts().sort_index()
        df_freq = pd.DataFrame({'sabine_count': freq_sabine, 'tom_count': freq_tom}).fillna(0).astype(int)

        df_crosstab = pd.crosstab(df_all['tom_type'], df_all['sabine_type'])
        all_types = list(range(0, 11))
        df_crosstab = df_crosstab.reindex(index=all_types, columns=all_types, fill_value=0)

        return df_all, df_no_both_zero, df_no_zero_ten, df_freq, df_crosstab

    def compute_cohen_kappa(self, y_true, y_pred, weights=None):
        """
        Berechnet Cohen's Kappa zwischen zwei Annotationarrays.
        weights: None, 'linear' oder 'quadratic'
        """
        # sklearn liefert den Kappa-Wert direkt
        return cohen_kappa_score(y_true, y_pred, weights=weights)

    def write_csv(self):
        """Schreibe alle Tabellen in eine einzige CSV-Datei und berechne Einigkeit mit Kappa."""
        self.load_annotations()
        df_all, df_no_both_zero, df_no_zero_ten, df_freq, df_crosstab = self.build_dataframes()

        # Alte Einigkeitsberechnung (prozentual)
        total = len(df_all)
        agree_all = (df_all['sabine_type'] == df_all['tom_type']).sum()
        percent_all = (agree_all / total * 100) if total > 0 else 0

        # Alte Einigkeit ohne bias_type_id == 10
        df_excl_ten = df_all[(df_all['sabine_type'] != 10) & (df_all['tom_type'] != 10)]
        total_excl = len(df_excl_ten)
        agree_excl = (df_excl_ten['sabine_type'] == df_excl_ten['tom_type']).sum()
        percent_excl = (agree_excl / total_excl * 100) if total_excl > 0 else 0

        # Neue Kappa-Berechnung
        kappa_unweighted = self.compute_cohen_kappa(df_all['sabine_type'], df_all['tom_type'])
        kappa_quadratic = self.compute_cohen_kappa(df_all['sabine_type'], df_all['tom_type'], weights='quadratic')

        with open(self.output_csv, 'w', encoding='utf-8', newline='') as f:
            f.write('--- Alle Annotationen ---\n')
            df_all.to_csv(f, index=False)
            f.write('\n')

            f.write('--- Ohne beide bias_type_id == 0 ---\n')
            df_no_both_zero.to_csv(f, index=False)
            f.write('\n')

            f.write('--- Ohne beide 0 und ohne Sabine bias_type_id == 10 ---\n')
            df_no_zero_ten.to_csv(f, index=False)
            f.write('\n')

            f.write('--- Häufigkeiten pro Annotator ---\n')
            df_freq.to_csv(f, index=True)
            f.write('\n')

            f.write('--- Kreuztabelle Tom (Zeilen) vs Sabine (Spalten) ---\n')
            df_crosstab.to_csv(f)
            f.write('\n')

            # Einigkeitsstatistiken
            f.write('--- Einigkeit der Annotatoren ---\n')
            f.write(f'Einigkeit (gesamt): {percent_all:.2f}% ({agree_all}/{total})\n')
            f.write(f'Einigkeit ohne bias_type_id==10: {percent_excl:.2f}% ({agree_excl}/{total_excl})\n')
            f.write(f"Cohen's Kappa (ohne Gewichtung): {kappa_unweighted:.3f}\n")
            f.write(f"Cohen's Kappa (quadratisch gewichtet): {kappa_quadratic:.3f}\n")

        print(f'CSV-Datei geschrieben: {self.output_csv}')


class InterAnnotatorAgreement:
    """
    A class to calculate and analyze inter-annotator agreement from a MongoDB database.
    It can also insert new annotations.
    """

    def __init__(self,
                 annotator1,
                 annotator2,
                 mongo_uri='mongodb://localhost:27017/',
                 db_name='court_decisions',
                 collection_name='judgments'):
        """
        Initializes the InterAnnotatorAgreement class.

        Args:
            annotator1 (str): The name of the first annotator.
            annotator2 (str): The name of the second annotator.
            mongo_uri (str): The URI for the MongoDB connection.
            db_name (str): The name of the database.
            collection_name (str): The name of the collection.
        """
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ismaster')
            print("MongoDB-Verbindung erfolgreich hergestellt.")
        except Exception as e:
            print(f"Fehler bei der Verbindung mit MongoDB: {e}")
            print("Bitte stellen Sie sicher, dass MongoDB läuft und unter der angegebenen URI erreichbar ist.")
            self.client = None
            return

        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.annotators = [annotator1, annotator2]
        self.comparison_data = defaultdict(lambda: {annotator: None for annotator in self.annotators})
        self.df = None

    def insert_annotations(self, annotations_to_insert):
        """
        Inserts new annotations into the database for a specific bias_id.

        Args:
            annotations_to_insert (list): A list of dicts, where each dict contains
                                          'bias_id' and 'annotation_data'.
        """
        if not self.client:
            return

        print("\n--- Füge neue Annotationen in die Datenbank ein ---")
        for item in annotations_to_insert:
            bias_id = item['bias_id']
            annotation_data = item['annotation_data']

            # Add a timestamp for consistency with existing data
            annotation_data['timestamp'] = time.time()
            annotation_data['bias_id'] = bias_id  # Ensure bias_id is in the annotation object

            query = {
                "ollama_responses.response.biases.id": bias_id
            }
            update = {
                "$push": {
                    "ollama_responses.$[].response.biases.$[elem].annotations": annotation_data
                }
            }
            array_filters = [
                {"elem.id": bias_id}
            ]

            try:
                result = self.collection.update_one(query, update, array_filters=array_filters)
                if result.matched_count > 0 and result.modified_count > 0:
                    print(f"Erfolg: Annotation für bias_id={bias_id} wurde hinzugefügt.")
                elif result.matched_count > 0 and result.modified_count == 0:
                    print(
                        f"Warnung: Dokument mit bias_id={bias_id} gefunden, aber nicht geändert. (Möglicherweise ein Fehler im Update-Pfad)")
                else:
                    print(f"Fehler: Kein Dokument mit bias_id={bias_id} gefunden.")
            except Exception as e:
                print(f"Ein Fehler ist beim Einfügen der Annotation für bias_id={bias_id} aufgetreten: {e}")
        print("-" * 50)

    def load_annotations(self):
        """
        Loads all relevant annotations for the specified annotators from the database
        using a MongoDB aggregation pipeline.
        """
        if not self.client:
            return

        print(f"\nLade Annotationen für: {self.annotators}...")
        pipeline = [
            {'$match': {
                'selected_for_smaller_experiment': True,
                'ollama_responses.response.biases.annotations.annotator': {'$in': self.annotators}
            }},
            {'$unwind': '$ollama_responses'},
            {'$unwind': '$ollama_responses.response.biases'},
            {'$unwind': '$ollama_responses.response.biases.annotations'},
            {'$match': {
                'ollama_responses.response.biases.annotations.annotator': {'$in': self.annotators}
            }},
            {'$project': {
                '_id': 0,
                'bias_id': '$ollama_responses.response.biases.id',
                'annotator': '$ollama_responses.response.biases.annotations.annotator',
                'bias_type_id': '$ollama_responses.response.biases.annotations.bias_type_id',
                'comment': '$ollama_responses.response.biases.annotations.comment'
            }}
        ]

        annotations = self.collection.aggregate(pipeline, allowDiskUse=True)
        self.comparison_data.clear()  # Clear previous data before loading
        count = 0
        for doc in annotations:
            bias_id = doc['bias_id']
            annotator = doc['annotator']
            self.comparison_data[bias_id][annotator] = {
                'bias_type_id': doc.get('bias_type_id', 0),
                'comment': doc.get('comment', '')
            }
            count += 1

        print(f"{count} Annotationen von den angegebenen Annotatoren gefunden.")

    def build_comparison_dataframe(self):
        """
        Builds a pandas DataFrame from the loaded annotation data for easy comparison.
        """
        if not self.comparison_data:
            print("Keine Vergleichsdaten gefunden.")
            return None

        rows = []
        annotator1, annotator2 = self.annotators
        for bias_id, annotations in self.comparison_data.items():
            ann1_data = annotations.get(annotator1) or {}
            ann2_data = annotations.get(annotator2) or {}
            rows.append({
                'bias_id': bias_id,
                f'{annotator1}_type': ann1_data.get('bias_type_id'),
                f'{annotator2}_type': ann2_data.get('bias_type_id'),
                f'{annotator1}_comment': ann1_data.get('comment', ''),
                f'{annotator2}_comment': ann2_data.get('comment', '')
            })

        df = pd.DataFrame(rows).sort_values(by='bias_id').reset_index(drop=True)
        self.df = df
        return df

    def calculate_and_print_agreement(self):
        """
        Calculates and prints the inter-annotator agreement statistics.
        """
        self.load_annotations()
        df = self.build_comparison_dataframe()

        if df is None or df.empty:
            print("Das DataFrame ist leer. Die Analyse kann nicht durchgeführt werden.")
            return

        df_complete = df.dropna(subset=[f'{self.annotators[0]}_type', f'{self.annotators[1]}_type'])
        if df_complete.empty:
            print("Keine vollständig annotierten Items für den Vergleich gefunden.")
            return

        y1 = df_complete[f'{self.annotators[0]}_type'].astype(int)
        y2 = df_complete[f'{self.annotators[1]}_type'].astype(int)

        total_items = len(df_complete)
        agreed_items = (y1 == y2).sum()
        agreement_percentage = (agreed_items / total_items * 100) if total_items > 0 else 0

        if len(pd.unique(pd.concat([y1, y2]))) < 2:
            kappa_unweighted = float('nan')
            kappa_quadratic = float('nan')
            print(
                "\nWarnung: Es sind weniger als zwei einzigartige Annotationsklassen vorhanden. Cohen's Kappa kann nicht berechnet werden.")
        else:
            kappa_unweighted = cohen_kappa_score(y1, y2)
            kappa_quadratic = cohen_kappa_score(y1, y2, weights='quadratic')

        crosstab = pd.crosstab(y1, y2)

        print("\n--- Analyse der Inter-Annotator-Übereinstimmung ---")
        print(f"Vergleich zwischen: '{self.annotators[0]}' und '{self.annotators[1]}'")
        print("-" * 50)
        print(f"Anzahl der gemeinsam annotierten Items: {total_items}")
        print(f"Prozentuale Übereinstimmung: {agreement_percentage:.2f}% ({agreed_items}/{total_items})")
        print(f"Cohen's Kappa (ungewichtet): {kappa_unweighted:.4f}")
        print(f"Cohen's Kappa (quadratisch gewichtet): {kappa_quadratic:.4f}")
        print("-" * 50)
        print("\n--- Kreuztabelle der Annotationen ---")
        print(f"Zeilen: {self.annotators[0]}, Spalten: {self.annotators[1]}")
        print(crosstab)
        print("-" * 50)

        df_disagree = df_complete[y1 != y2]
        if not df_disagree.empty:
            print("\n--- Items mit abweichenden Annotationen ---")
            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                print(df_disagree)
        else:
            print("\nPerfekte Übereinstimmung! Keine abweichenden Annotationen gefunden.")

    def print_annotation_counts_for_annotator(self, annotator_name):
        """
        Calculates and prints the frequency of each bias_type_id for a single annotator.
        """
        print(f"\n--- Auszählung der Annotationen für: {annotator_name} ---")
        if self.df is None or self.df.empty:
            print("Keine Daten zum Analysieren vorhanden. Führe zuerst 'calculate_and_print_agreement' aus.")
            return

        annotator_column = f'{annotator_name}_type'
        if annotator_column not in self.df.columns:
            print(f"Fehler: Spalte '{annotator_column}' nicht im DataFrame gefunden.")
            return

        counts = self.df[annotator_column].dropna().astype(int).value_counts()
        all_bias_ids = range(11)
        counts = counts.reindex(all_bias_ids, fill_value=0)

        print(f"Häufigkeit der 'bias_type_id' für '{annotator_name}':")
        print(counts.to_string())
        print("-" * 50)



if __name__ == '__main__':
    #comparer = AnnotationComparer()
    #comparer.write_csv()

    ANNOTATOR_1 = 'Sabine Wehnert' # Ersetzt 'Tom Herzberg' aus dem Beispiel
    ANNOTATOR_2 = 'Kilian Lüders'

    analyzer = InterAnnotatorAgreement(annotator1=ANNOTATOR_1, annotator2=ANNOTATOR_2)
    if analyzer.client:
        # 1. Neue Annotationen für Kilian Lüders definieren und einfügen
        annotations_for_kilian = [
            {
                'bias_id': 41,
                'annotation_data': {
                    'annotator': 'Kilian Lüders',
                    'bias_type_id': 0,
                    'comment': 'Automatisch nachgetragen'
                }
            },
            {
                'bias_id': 48,
                'annotation_data': {
                    'annotator': 'Kilian Lüders',
                    'bias_type_id': 0,
                    'comment': 'Automatisch nachgetragen'
                }
            }
        ]
        analyzer.insert_annotations(annotations_for_kilian)

        # 2. Die Übereinstimmungsanalyse durchführen (lädt die Daten neu)
        analyzer.calculate_and_print_agreement()

        # 3. Die Zählung für den Annotator ausgeben
        analyzer.print_annotation_counts_for_annotator(ANNOTATOR_2)



