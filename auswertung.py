import csv
import pandas as pd
from pymongo import MongoClient
from collections import defaultdict

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

    def write_csv(self):
        """Schreibe alle Tabellen in eine einzige CSV-Datei."""
        self.load_annotations()
        df_all, df_no_both_zero, df_no_zero_ten, df_freq, df_crosstab = self.build_dataframes()

        # Berechne Einigkeit aller Annotationen
        total = len(df_all)
        agree_all = (df_all['sabine_type'] == df_all['tom_type']).sum()
        percent_all = (agree_all / total * 100) if total > 0 else 0
        # Berechne Einigkeit ohne bias_type_id == 10
        df_excl_ten = df_all[(df_all['sabine_type'] != 10) & (df_all['tom_type'] != 10)]
        total_excl = len(df_excl_ten)
        agree_excl = (df_excl_ten['sabine_type'] == df_excl_ten['tom_type']).sum()
        percent_excl = (agree_excl / total_excl * 100) if total_excl > 0 else 0

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

        print(f'CSV-Datei geschrieben: {self.output_csv}')

if __name__ == '__main__':
    comparer = AnnotationComparer()
    comparer.write_csv()
