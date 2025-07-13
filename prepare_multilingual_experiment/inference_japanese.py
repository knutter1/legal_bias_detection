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
RUN_ID = 8
TEST_ONLY = True
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
ドイツ語の評価テキストが提供されます。課題は、このテキストを分析し、以下の種類の偏見がないか確認し、なぜこの文章に偏見が含まれているのかを明確に説明することです。
以下の定義と判別基準を使用してください。

1. ジェンダーバイアス
ジェンダーバイアスとは、性別に基づく体系的かつ不平等な扱いを指します。
ジェンダーバイアスの特徴：
- 構造的バイアス：ステレオタイプ的な思い込みを助長する文法構造の使用。
- 文脈的バイアス：ジェンダー役割やステレオタイプを強化する特定の単語や語調の使用。
- ステレオタイプ化：社会的なジェンダー役割に基づいて、特性や職業を割り当てること。
ジェンダーバイアスを特定するには、次の方法があります。
- 語の連想を分析する（例：女性を表す肯定的な形容詞は、身体的特徴を指すことが多い）。
- 専門用語におけるステレオタイプを確認する。
- ジェンダー関連の一般化について文法構造を分析する。

2. 宗教的偏見
宗教的偏見とは、しばしば意識の奥底に潜む、暗黙の態度や偏見を指し、宗教間の協力を妨げる可能性があります。
特徴：
- 宗教的偏見は、地位、聖典、国境を越えた影響といった社会心理学的な差異に基づく場合があります。
- 集団偏愛の傾向。内集団は肯定的に、外集団は否定的に描写されます。
- 宗教的文献への愛着の違いは、神学的に異なる集団に対する認識を強める可能性があります。
これを特定するには、以下の方法があります。
- 暗黙の偏見を調査する。
- 宗教間のメッセージに対する反応の違いを、メッセージの内容と情報源に基づいて調べる。
- 特定の宗教集団に肯定的または否定的な属性を帰属させるまでの遅延を測定する。

3. 人種的偏見
人種差別的偏見は、暗黙的（比較的無意識的）または明示的（意識的）に現れます。暗黙的および明示的な偏見はどちらも広く蔓延しており、強い否定的な結果をもたらします。
特徴：
- 人種的偏見は、カテゴリー分け、ステレオタイプ化、偏見、差別によって強化されます。
- 偏見は、意図がない場合でも、歪んだ判断や差別的な行動につながる可能性があります。
以下の点から認識しましょう。
- 社会的な交流や職場での交流におけるステレオタイプの存在。
- 不確実な状況や時間的制約のある状況における歪んだ判断や行動。
- 集団の特性に基づく個人の扱いの違い。

4. 性的指向に基づく偏見
定義：
性的指向に基づく偏見とは、個人の性的嗜好に基づいて、意識的または無意識的に差別することです。職場、教育機関、その他の社会的な場において、これらの個人に不利益となるような決定や行動が伴います。
特徴：
- 性的マイノリティに対するステレオタイプや偏見に基づく差別。
- 雇用、昇進、その他の職務上の決定における不平等な扱い。
- 平等な権利やサービスの否定。
以下の点に注意して認識してください。
- 同様の状況下における同性愛者と異性愛者の扱いを比較する。
- 暗黙的または明示的に偏見を反映する言葉や行動を分析する。
- 特定の集団に不利益となる規則を不均衡に適用する。

5. 年齢差別
年齢差別とは、正当な社会政策目標または客観的な理由によって正当化されない限り、年齢に基づくあらゆる形態の不平等な扱いまたは不利益を包含します。直接差別と間接差別の両方が含まれます。
特徴：
- 年齢差別は、社会政策規制における特定の年齢制限の適用によって異なります。
- 比例性テストは、年齢制限が不平等な場合に特に重要です。
以下の点に注意して認識してください。
- 意図された目標と客観的な関連性のない、厳格に定義された年齢制限によって集団に不利益を与えること。
- 年齢制限によって特定の社会保障へのアクセスが妨げられるケース。
- 失業保険の対象年齢層が限定されているなどの指標。

6. 国籍バイアス
国籍バイアスとは、国やその国民が不正確、ステレオタイプ的、あるいは軽蔑的な形で描写される、体系的な歪曲を指します。
特徴：
- 特定の国籍に対するステレオタイプ的または軽蔑的な表現。
- 特定の国の軍事紛争や政情不安に焦点を当てた話題。
認識すべき点：
- 暴力、テロ、汚職といった話題が、特定の国に関して過剰に取り上げられている。

7. 障がい者バイアス
障がい者に対するバイアスとは、自動的に以下を指します。障害のある人が否定的に認識されたり、扱われたりする原因となる、無意識のうちに活性化された態度。これらの態度は、社会的な固定観念や連想に基づいており、しばしば差別的な行動につながります。
特徴：
- 障害のある人よりも、障害のない人を好むという、暗黙の否定的な選好。
- 障害を否定的な言葉（例：悪い、無能）と自動的に結びつける。
- 障害のある人を子供っぽい、あるいは能力が劣っていると認識する傾向。
以下の方法でこれを認識します。
- 文章中における「障害」と「否定的」といった言葉の関連性を分析する。
- 能力や自律性の欠如を示唆する微妙な表現を特定する。
- 障害のある人に不利な意思決定プロセスにおける暗黙の前提を検証する。

8. 外見バイアス
外見の魅力は、望ましい特性の指標としてよく使われるヒューリスティックです。研究によると、人々は魅力的な人物に道徳的資質を帰属させる傾向が、魅力のない人物に帰属させる傾向よりも強く、この影響は魅力的な人物を肯定的な非道徳的資質と結びつける傾向よりも強いことが示されています。これは、外見的な魅力が特に道徳的性格の認識に強く影響することを示唆しています。
特性：
- 魅力的な人物は道徳的であると認識される可能性が高い。
- 道徳的資質の帰属は、非道徳的資質の帰属よりも強い。
- 道徳的資質の認識は、迅速なヒューリスティック評価に基づいています。
これを検出するには、次の方法があります。
- 誠実さや信頼性といった道徳的資質が外見的な魅力と関連しているかどうかを確認する。
- 魅力的な人物に社会的望ましさを過度に投影する評価を分析する。
- 魅力的な人物と魅力のない人物の道徳的および非道徳的帰属パターンを比較する。

9. 社会経済的地位バイアス：
社会経済的地位バイアスとは、人々が経済的・社会的地位に基づいて不平等に扱われたり判断されたりすることで生じる体系的な歪みを指し、機会均等を阻害します。
特徴
- 教育や就職機会へのアクセスにおける歪み。
- 収入や富に基づく認識や扱いの違い。
- 地位に関する主観的な認識が意思決定に与える影響。
これを認識するには、次のことを行います。
- 教育、収入、または政治参加における社会経済的グループ間の格差を分析する。
- 同等の能力やリソースがあるにもかかわらず、不平等な結果が生じる状況を特定する。
- 社会流動性と構造的障壁を観察する。

判決文：

{text}

判決文終了
出力形式：
判決に上記のバイアスが一切含まれていない場合は、「バイアスなし」と回答し、判決の理由は記載しないでください。それ以外の場合は、以下のように回答してください。
特定の種類のバイアスを特定した場合は、私がさらに深く検討できるよう、回答を3つの部分に分けた形式で構成してください。
回答は連続した文章で、以下の形式で提出してください。
特定したバイアス：[ここでは、バイアスの種類（「ジェンダーバイアス」、「宗教バイアス」、「人種バイアス」、「性的指向バイアス」、「年齢差別」、「国籍バイアス」、「障害バイアス」、「外見バイアス」、「社会経済的地位バイアス」のいずれか）を明記してください。]
本文：[ここでは、判決文から関連箇所を引用してください。]
根拠：[なぜこの箇所がこのバイアスを示しているのか説明してください。]
判決文に複数のバイアスが含まれている場合は、この形式で複数回回答してください。
"""

    start_time = time.time()
    try:
        response = query_ollama(model, prompt_text, gpu_nr=gpu_nr, num_ctx=num_ctx)
    except Exception as e:
        logging.error(f"Fehler bei der Modellabfrage: {e}")
        return []

    elapsed_time = round(time.time() - start_time, 2)

    return {
        "id": str(uuid.uuid4()),
        "model": model,
        "response": response,
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
    query = {"language": "Japanese"}
    if SKIP_PROCESSED:
        query["ollama_responses.run_id"] = {"$ne": run_id}

    # Elemente abrufen und nach num_characters sortieren
    elements = list(collection.find(query))
    elements.sort(key=lambda x: len(x["full_text"]))

    # Aufteilen in Teilarrays nach Kontextgrößen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (len(element["full_text"]) + 5000) // 1     # +5000, weil Promptgerüst
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
            text = element["full_text"]

            processed_count = collection.count_documents(
                {"ollama_responses.run_id": RUN_ID, "language": "Japanese"})

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
