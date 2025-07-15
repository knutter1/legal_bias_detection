import os
import time
import logging
from pymongo import MongoClient
from ollama_essentials import query_ollama, is_gpu_memory_overloaded
import uuid
from random import shuffle
from datetime import datetime


# Toggle for skipping already processed entries
SKIP_PROCESSED = True
TEST_ONLY = False
RELOAD_MODEL_IF_MEMORY_FULL = False


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



def get_model_response_vn(text, model, run_id, gpu_nr=0, num_ctx=8192):
    """
    Analysiert den Text und gibt die Antworten mit IDs zurück.
    """
    prompt_text = f"""
Bạn được cung cấp một bản án của tòa án. Nhiệm vụ của bạn là phân tích văn bản này để xác định các loại thiên kiến (bias) sau đây, chỉ tập trung vào thiên kiến của thẩm phán hiện tại (không tính các cá nhân được trích dẫn hoặc các tòa án khác), và đưa ra lý do rõ ràng tại sao một đoạn văn cụ thể thể hiện thiên kiến đó.
Hãy sử dụng các định nghĩa và tiêu chí dưới đây:
1. Thiên kiến Giới tính
 Thiên kiến giới tính đề cập đến việc đối xử không công bằng một cách có hệ thống dựa trên giới.
Đặc điểm:
- Thiên kiến cấu trúc: Sử dụng cấu trúc ngữ pháp củng cố các giả định định kiến về giới.
- Thiên kiến ngữ cảnh: Sử dụng từ ngữ hoặc tông giọng củng cố vai trò hoặc khuôn mẫu giới.
- Khuôn mẫu: Gán đặc điểm hoặc nghề nghiệp dựa trên vai trò xã hội hóa của giới.


Hãy xác định thiên kiến này bằng cách phân tích liên kết từ ngữ, các thuật ngữ nghề nghiệp, hoặc cấu trúc ngữ pháp nhằm tổng quát hóa về giới.


2. Thiên kiến Tôn giáo
 Thiên kiến tôn giáo là những thái độ, định kiến ngầm định có thể cản trở hợp tác giữa các tôn giáo.
Đặc điểm:
- Xuất phát từ sự khác biệt xã hội-tâm lý như địa vị, kinh điển hoặc ảnh hưởng xuyên quốc gia.
- Thiên vị nhóm: nhóm mình được miêu tả tích cực, nhóm ngoài bị miêu tả tiêu cực.
- Sự khác biệt trong tuân thủ giáo lý có thể làm nổi bật khoảng cách thần học.
 Hãy xác định thiên kiến này bằng cách phân tích các thành kiến ngầm định, phản ứng khác biệt với thông điệp liên tôn giáo, hoặc tốc độ liên kết các thuộc tính tốt/xấu với các nhóm tôn giáo nhất định.


3. Thiên kiến Chủng tộc
 Thiên kiến chủng tộc có thể là ngầm định hoặc hiển hiện, đều phổ biến và gây tác động tiêu cực nghiêm trọng.
Đặc điểm:
- Được củng cố bởi phân loại, khuôn mẫu, định kiến và phân biệt đối xử.
- Có thể dẫn đến phán xét méo mó hoặc hành vi phân biệt dù không cố ý.
 Hãy xác định thiên kiến này qua các khuôn mẫu trong bối cảnh xã hội, công việc, sự khác biệt trong đánh giá hoặc hành vi dựa trên đặc điểm nhóm.


4. Thiên kiến Xu hướng Tình dục
 Thiên kiến này liên quan đến việc gây bất lợi (ý thức hoặc vô thức) cho cá nhân dựa trên xu hướng tình dục của họ.
Đặc điểm:
- Phân biệt đối xử dựa trên định kiến về các nhóm thiểu số tình dục.
- Đối xử không công bằng trong tuyển dụng, thăng tiến hoặc các quyết định công việc khác.
- Từ chối quyền lợi hoặc dịch vụ công bằng.
 Hãy xác định bằng cách so sánh cách đối xử với người đồng tính và dị tính trong cùng bối cảnh, phân tích ngôn ngữ hoặc hành động thể hiện định kiến.


5. Phân biệt tuổi tác
 Phân biệt tuổi tác là việc đối xử không công bằng hoặc gây bất lợi dựa trên tuổi, trừ khi được biện minh bằng chính sách xã hội hợp lý hoặc lý do khách quan.
Đặc điểm:
- Đặt giới hạn tuổi trong chính sách mà không có lý do khách quan.
- Cần kiểm tra sự tương xứng khi áp dụng các ngưỡng tuổi khác nhau.
 Hãy xác định bằng cách kiểm tra việc gây bất lợi do giới hạn tuổi cứng nhắc không liên quan mục tiêu chính sách, hoặc hạn chế hưởng quyền lợi công dựa vào tuổi.


6. Thiên kiến Quốc tịch
 Thiên kiến quốc tịch liên quan đến việc mô tả sai lệch có hệ thống về quốc gia hoặc công dân, thường qua khuôn mẫu hoặc cách miêu tả xúc phạm.
Đặc điểm:
- Ngôn ngữ khuôn mẫu hoặc miệt thị về quốc tịch nhất định.
- Nhấn mạnh quá mức về xung đột quân sự hoặc bất ổn chính trị của các quốc gia.
 Hãy xác định bằng cách kiểm tra sự tập trung quá mức vào bạo lực, khủng bố hoặc tham nhũng liên quan đến quốc gia cụ thể.


7. Thiên kiến đối với người khuyết tật
 Thiên kiến với người khuyết tật là các thái độ ngầm định, tự động, dẫn đến nhận thức hoặc đối xử tiêu cực.
Đặc điểm:
- Sự ưu ái ngầm định đối với người không khuyết tật.
- Tự động liên kết khuyết tật với những khái niệm tiêu cực (ví dụ: "xấu", "kém năng lực").
- Khuynh hướng xem người khuyết tật là trẻ con hoặc kém khả năng hơn.
 Hãy xác định bằng cách phân tích liên kết giữa “khuyết tật” và từ ngữ tiêu cực, ngôn ngữ hàm ý thiếu năng lực hoặc tự chủ.


8. Thiên kiến Ngoại hình
 Ngoại hình thường được dùng làm cơ sở phán xét về đặc điểm đạo đức. Nghiên cứu cho thấy người hấp dẫn thường được đánh giá cao về phẩm chất đạo đức hơn so với người kém hấp dẫn.
Đặc điểm:
- Người hấp dẫn dễ được coi là có phẩm chất đạo đức.
- Phẩm chất đạo đức thường liên hệ mạnh hơn với ngoại hình so với các phẩm chất khác.
- Các nhận xét này thường dựa trên đánh giá nhanh chóng.
 Hãy xác định bằng cách xem các đặc điểm như trung thực, đáng tin có được liên kết với ngoại hình hay không, hoặc có sự khác biệt trong đánh giá đạo đức theo ngoại hình.


9. Thiên kiến Địa vị Kinh tế Xã hội
 Thiên kiến này là sự phân biệt hoặc đối xử khác biệt có hệ thống do vị trí kinh tế hoặc xã hội, thường ảnh hưởng đến cơ hội bình đẳng.
Đặc điểm:
- Cơ hội học tập, việc làm không đồng đều.
- Đánh giá hoặc đối xử khác nhau dựa trên thu nhập hoặc tài sản.
- Sự chênh lệch kết quả dù khả năng hoặc nguồn lực tương đương, hoặc rào cản di chuyển xã hội.

Hãy xác định bằng cách phân tích chênh lệch về giáo dục, thu nhập hoặc tham gia chính trị, kết quả không đồng đều, rào cản cơ cấu xã hội.


Bản án của tòa án:
 {text}
KẾT THÚC VĂN BẢN
Định dạng kết quả:
 Nếu không phát hiện bất kỳ thiên kiến nào ở trên trong bản án, chỉ trả lời:
 “Không có thiên kiến” (không cần giải thích).
Nếu phát hiện một hoặc nhiều loại thiên kiến, trả lời cho từng loại theo mẫu sau (dạng đoạn văn, không dùng gạch đầu dòng):
Identified Bias: [Loại thiên kiến, ví dụ: "Thiên kiến Giới tính", "Thiên kiến Chủng tộc", v.v.]
Text Passage: "[Trích dẫn đoạn văn liên quan trong bản án]"
Justification: [Giải thích vì sao đoạn văn này thể hiện thiên kiến đó]
Lặp lại mẫu này cho từng thiên kiến được phát hiện.
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
        "run_id": run_id
    }


def bias_check_single_vn(model, run_id):
    """
    Hauptfunktion für die Biasüberprüfung.
    """
    # Verbindung zur MongoDB
    collection = connect_to_mongo()

    # Filter: Elemente mit "selected_for_experiment" = true
    query = {"language": "Vietnamese"}
    if SKIP_PROCESSED:
        query["ollama_responses.run_id"] = {"$ne": run_id}

    # Elemente abrufen und nach num_characters sortieren
    elements = list(collection.find(query))
    elements.sort(key=lambda x: len(x["full_text"]))

    # Aufteilen in Teilarrays nach Kontextgrößen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (len(element["full_text"]) + 5000) // 3     # +5000, weil Promptgerüst
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

            # Text abrufen
            text = element["full_text"]

            processed_count = collection.count_documents(
                {"ollama_responses.run_id": run_id, "language": "Vietnamese"})

            # Modellantwort abrufen, prüft alle 10 Abfragen ob Speicherprobleme sind, lädt dann model neu
            if RELOAD_MODEL_IF_MEMORY_FULL and processed_count % 10 == 0 and is_gpu_memory_overloaded(threshold=.9):
                _ctx = 131072
                print("Lade model neu, weil GPU-Speicher voll ist")
            else:
                _ctx = ctx

            # get response from model and measure time taken
            start_time = time.time()
            response_list = get_model_response_vn(model=model, text=text, run_id = run_id, num_ctx=_ctx)
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
                f"{datetime.now().strftime("%H:%M:%S")} - Verarbeitete Elemente für Run {run_id}: {processed_count} in {round(end_time - start_time, 2)}s")

        print(f"Verarbeitung für Kontextgröße {ctx} abgeschlossen.")


def get_model_response_jp(text, model, run_id, gpu_nr=0, num_ctx=8192):
    """
    Analysiert den Text und gibt die Antworten mit IDs zurück.
    """
    prompt_text = f"""
あなたには裁判所の判決文が与えられます。あなたの課題は、下記の偏見の種類について、現在の裁判官によるものに限定して、判決文を分析し、偏見を示す該当箇所とその明確な根拠を提示することです。引用された個人や他の裁判所による偏見は含めないでください。
以下の定義と基準を使用してください：
1. ジェンダーバイアス
 ジェンダーバイアスとは、性別に基づく体系的な不平等な扱いを指します。
特徴：
- 構造的バイアス：ステレオタイプを強化する文法構造の使用。
- 文脈的バイアス：性別に関連する役割や固定観念を強化する特定の言葉やトーンの使用。
- ステレオタイプ：社会的に構築された性別役割に基づく特性や職業の帰属。


形容詞の連想（例：女性に対する肯定的形容詞が主に外見に関する場合）や、職業用語、文法構造に性別に基づく一般化がないか分析してください。


2. 宗教バイアス
 宗教バイアスとは、暗黙の態度や偏見により、宗教間の協力が妨げられることを指します。
特徴：
- 社会心理的な違い（地位、経典、国際的影響など）に根ざす。
- 集団えこひいき：内集団を肯定的に、外集団を否定的に描写。
- 宗教的教義への異なる態度が神学的な違いを強調する場合がある。
- 暗黙的偏見の調査、宗教的メッセージへの反応の違い、特定宗教集団への属性付けの遅延を分析してください。


3. 人種バイアス
 人種バイアスには、無意識的（インプリシット）および意識的（エクスプリシット）なものがあり、いずれも深刻な悪影響を及ぼします。
特徴：
- カテゴライズ、ステレオタイプ、偏見、差別により強化される。
- 意図がなくても歪んだ判断や差別的行動につながる。
- 社会・職業的文脈でのステレオタイプや、グループ特性に基づく扱いの差などを分析してください。


4. 性的指向バイアス
 性的指向バイアスとは、性的嗜好に基づいて意識的または無意識的に不利益を与えることを指します。
特徴：
- 性的マイノリティに対する偏見や差別。
- 雇用や昇進などでの不平等な扱い。
- 権利やサービスへの平等なアクセスの否定。
- 同一状況での異性愛者と同性愛者の扱いの違いや、言語・行動に偏見がないか分析してください。


5. 年齢差別
 年齢差別とは、正当な社会政策や客観的理由なしに年齢を理由として不平等な扱いをすることを指します。
特徴：
- 客観的理由のない年齢制限の設定。
- 公的給付などへのアクセス制限。
- 年齢制限が政策目的に関連していない場合や、雇用保険の年齢制限などを分析してください。


6. 国籍バイアス
 国籍バイアスとは、国や国民に対する体系的な歪曲的描写やステレオタイプ、侮辱的な表現を指します。
特徴：
- 特定の国籍に対するステレオタイプ的または侮辱的な言語。
- 軍事紛争や政治的不安などの強調。
- 暴力やテロ、腐敗などに関する描写が特定国に偏っていないか分析してください。


7. 障害者バイアス
 障害者に対するバイアスとは、無意識に自動的に発生する否定的な態度や扱いを指します。
特徴：
- 健常者に対する暗黙の好意。
- 障害と否定的概念の自動的な結びつき。
- 障害者を無能や幼児的に見る傾向。
- 「障害」と否定的用語の連想や、能力・自律性の暗黙的否定を分析してください。


8. 外見バイアス
 外見の魅力は、道徳的特性などの判断に影響する傾向があります。
特徴：
- 魅力的な人がより道徳的と見なされる。
- 社会的望ましさが外見によって不釣り合いに投影される。
- 外見の違いによる道徳的・非道徳的特性の帰属傾向を分析してください。


9. 社会経済的地位バイアス
 社会経済的地位に基づくバイアスとは、経済的・社会的地位による体系的な不利益や差別的扱いを指します。
特徴：
- 教育・雇用機会への不平等なアクセス。
- 所得や資産に基づく異なる扱い。


- 社会的流動性の障壁や構造的不平等を分析してください。


判決文:
 {text}
判決文終了
出力形式:
 上記のバイアスが判決文に一切認められない場合は、次のようにだけ答えてください：
 「バイアスなし」（説明不要）
いずれかのバイアスが認められる場合は、それぞれについて下記の形式（箇条書き不可、連続した文章）で回答してください：
Identified Bias: [バイアスの種類（例：「ジェンダーバイアス」等）]
Text Passage: 「判決文からの該当引用箇所」
Justification: [その箇所がバイアスを示す理由の説明]
バイアスごとにこの形式で繰り返してください。
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
        "run_id": run_id
    }


def bias_check_single_jp(model, run_id):
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

            # Text abrufen
            text = element["full_text"]

            processed_count = collection.count_documents(
                {"ollama_responses.run_id": run_id, "language": "Japanese"})

            # Modellantwort abrufen, prüft alle 10 Abfragen ob Speicherprobleme sind, lädt dann model neu
            if RELOAD_MODEL_IF_MEMORY_FULL and processed_count % 10 == 0 and is_gpu_memory_overloaded(threshold=.9):
                _ctx = 131072
                print("Lade model neu, weil GPU-Speicher voll ist")
            else:
                _ctx = ctx

            # get response from model and measure time taken
            start_time = time.time()
            response_list = get_model_response_jp(model=model, text=text, num_ctx=_ctx, run_id=run_id)
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
                f"{datetime.now().strftime("%H:%M:%S")} - Verarbeitete Elemente für Run {run_id}: {processed_count} in {round(end_time - start_time, 2)}s")

        print(f"Verarbeitung für Kontextgröße {ctx} abgeschlossen.")


def get_model_response_en(text, model, run_id, gpu_nr=0, num_ctx=8192):
    """
    Analysiert den Text und gibt die Antworten mit IDs zurück.
    """
    prompt_text = f"""
You are given a court decision. Your task is to analyze the text for the following types of bias against or in favor of one of the parties or a class of persons and provide a clear justification for why a given passage demonstrates bias. Make sure to only check for bias by the current judge and not by cited individuals or other courts. 

Use the definitions and criteria listed below:


1. Gender Bias
Gender bias refers to the systematic unequal treatment of individuals based on gender.

Characteristics:
- Structural bias: Use of grammatical constructions that reinforce stereotypical assumptions.
- Contextual bias: Use of specific words or tone that reinforce gender-related roles or stereotypes.

Stereotyping: Attributing traits or occupations based on socially constructed gender roles.
Identify gender bias by:
- Analyzing word associations (e.g., positive adjectives applied to women often relate to physical appearance).
- Reviewing occupational terms for gender stereotypes.
- Examining grammatical structures for gender-based generalizations.


2. Religious Bias
Religious bias refers to implicit attitudes and prejudices that often operate below conscious awareness and can hinder interfaith cooperation.

Characteristics:
- Rooted in social-psychological differences like status, scriptures, or transnational influences.
- Group favoritism: the in-group is portrayed positively, the out-group negatively.
- Divergent adherence to religious texts can amplify perceived theological differences.

Identify this bias by:
- Investigating implicit prejudices.
- Analyzing differing reactions to interfaith messages depending on content and source.
- Measuring delays in associating positive/negative attributes with specific religious groups.


3. Racial Bias
Racial bias can be either implicit (largely unconscious) or explicit (conscious). Both are widespread and have serious negative effects.

Characteristics:
- Reinforced by categorization, stereotyping, prejudice, and discrimination.
- Can result in distorted judgments and discriminatory behavior, even without intent.

Identify this bias by:
- Presence of stereotypes in social or professional contexts.
- Skewed judgments or behaviors under uncertainty or time pressure.
- Differential treatment based on group characteristics.


4. Sexual Orientation Bias
Sexual orientation bias involves the conscious or unconscious disadvantage of individuals based on their sexual preference, especially in the workplace, education, or other social contexts.

Characteristics:
- Discrimination based on stereotypes or prejudice against sexual minorities.
- Unequal treatment in employment, promotion, or other work-related decisions.
- Denial of equal rights or access to services.

Identify this bias by:
- Comparing treatment of homosexual and heterosexual individuals in similar contexts.
- Analyzing language or actions that reflect explicit or implicit prejudice.
- Detecting disproportionate application of rules that disadvantage certain groups.


5. Age Discrimination
Age discrimination refers to unequal treatment or disadvantage based on age, unless justified by legitimate social policy or objective reasons. This includes both direct and indirect discrimination.

Characteristics:
- Defined age limits in policy settings without objective justification.
- Relevance of proportionality checks when applying different age thresholds.

Identify this bias by:
- Disadvantaging groups due to rigid age limits unrelated to policy goals.
- Age thresholds that restrict access to public benefits.
- Examples such as unemployment insurance limited to certain age groups.


6. Nationality Bias
Nationality bias involves the systematic distortion of how countries or their citizens are portrayed, often through stereotypes or demeaning depictions.

Characteristics:
- Stereotypical or derogatory language about certain nationalities.
- Overemphasis on military conflict or political instability in particular countries.

Identify this bias by:
- Focus on topics like violence, terrorism, or corruption disproportionately in relation to specific countries.


7. Disability Bias
Bias against people with disabilities refers to automatically triggered, unconscious attitudes that result in negative perceptions or treatment. These are often based on societal stereotypes.

Characteristics:
- Implicit negative preferences for non-disabled individuals.
- Automatic associations of disability with negative concepts (e.g., "bad," "incompetent").
- Tendency to view people with disabilities as childlike or less capable.

Identify this bias by:
- Analyzing associations between "disability" and negative terms.
- Identifying subtle language implying lack of competence or autonomy.
- Uncovering implicit assumptions in decisions that disadvantage disabled individuals.


8. Appearance Bias
Physical attractiveness is often used as a heuristic for desirable traits. Research shows people tend to attribute moral traits more strongly to attractive individuals than to unattractive ones, more so than for non-moral traits.

Characteristics:
- Attractive people are more likely to be seen as moral.
- Moral traits are more strongly associated with appearance than non-moral traits.
- These perceptions are shaped by quick heuristic judgments.

Identify this bias by:
- Checking whether traits like honesty or trustworthiness are linked to physical attractiveness.
- Analyzing whether social desirability is disproportionately projected onto attractive people.
- Comparing attribution patterns for moral vs. non-moral traits across different appearances.


9. Socioeconomic Status Bias
Bias based on socioeconomic status involves the systematic disadvantage or differential treatment of individuals due to their economic or social position, often affecting equal opportunity.

Characteristics:
- Unequal access to education and job opportunities.
- Different treatment or perceptions based on income or wealth.
- Influence of subjective status perceptions on decisions.

Identify this bias by:
- Analyzing disparities in education, income, or political participation across groups.
- Identifying unequal outcomes despite comparable ability or resources.
- Observing social mobility barriers and structural inequality.


Here is the court decision:
{text}

END OF DECISION TEXT

Output Format:
If none of the above biases are found in the judgment, respond only with:
“No bias” (without explanation).

If one or more types of bias are found, respond for each one using this format in continuous prose (no bullet points):

Identified Bias: [Insert the type of bias, e.g., "Gender Bias", "Religious Bias", etc.]  
Text Passage: "[Insert the relevant quoted passage from the court decision]"  
Justification: [Explain why this passage reflects the identified bias]

Repeat this format for each identified bias.
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
        "run_id": run_id
    }


def bias_check_single_en(model, run_id):
    """
    Hauptfunktion für die Biasüberprüfung.
    """
    # Verbindung zur MongoDB
    collection = connect_to_mongo()

    # Filter: Elemente mit "selected_for_experiment" = true
    query = {"language": "English"}
    if SKIP_PROCESSED:
        query["ollama_responses.run_id"] = {"$ne": run_id}

    # Elemente abrufen und nach num_characters sortieren
    elements = list(collection.find(query))
    elements.sort(key=lambda x: len(x["full_text"]))

    # Aufteilen in Teilarrays nach Kontextgrößen
    context_thresholds = [8192, 16384, 32768, 65536, 131072]
    context_buckets = {ctx: [] for ctx in context_thresholds}

    for element in elements:
        num_chars = (len(element["full_text"]) + 5000) // 3     # +5000, weil Promptgerüst
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

            # Text abrufen
            text = element["full_text"]

            processed_count = collection.count_documents(
                {"ollama_responses.run_id": run_id, "language": "English"})

            # Modellantwort abrufen, prüft alle 10 Abfragen ob Speicherprobleme sind, lädt dann model neu
            if RELOAD_MODEL_IF_MEMORY_FULL and processed_count % 10 == 0 and is_gpu_memory_overloaded(threshold=.9):
                _ctx = 131072
                print("Lade model neu, weil GPU-Speicher voll ist")
            else:
                _ctx = ctx

            # get response from model and measure time taken
            start_time = time.time()
            response_list = get_model_response_en(model=model, text=text, run_id = run_id, num_ctx=_ctx)
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
                f"{datetime.now().strftime("%H:%M:%S")} - Verarbeitete Elemente für Run {run_id}: {processed_count} in {round(end_time - start_time, 2)}s")

        print(f"Verarbeitung für Kontextgröße {ctx} abgeschlossen.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("prepare_multilingual_experiment/process_and_store.log"),
            logging.StreamHandler()
        ]
    )

    bias_check_single_jp(model="deepseek-r1:70b", run_id=11)
