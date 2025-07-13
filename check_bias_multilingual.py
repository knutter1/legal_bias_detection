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



def get_model_response_vn(text, model, run_id, gpu_nr=0, num_ctx=8192):
    """
    Analysiert den Text und gibt die Antworten mit IDs zurück.
    """
    prompt_text = f"""
Bạn sẽ được cung cấp một văn bản đánh giá bằng tiếng Đức. Nhiệm vụ của bạn là phân tích văn bản này để tìm các loại định kiến ​​sau và đưa ra lời giải thích chính xác về lý do tại sao đoạn văn có định kiến.
Sử dụng các định nghĩa và tiêu chí phân biệt bên dưới:

1. Định kiến ​​giới
Định kiến ​​giới mô tả sự đối xử có hệ thống, không bình đẳng dựa trên giới tính.
Đặc điểm của định kiến ​​giới:
- Định kiến ​​cấu trúc: Sử dụng các cấu trúc ngữ pháp thúc đẩy các giả định khuôn mẫu.
- Định kiến ​​ngữ cảnh: Sử dụng các từ hoặc giọng điệu cụ thể củng cố vai trò và khuôn mẫu giới.
- Định kiến: Gán các đặc điểm hoặc nghề nghiệp dựa trên vai trò giới tính xã hội.
Xác định định kiến ​​giới bằng cách:
- Phân tích các liên tưởng từ ngữ (ví dụ: tính từ tích cực dành cho phụ nữ thường đề cập đến các đặc điểm ngoại hình).
- Kiểm tra các định kiến ​​theo thuật ngữ chuyên môn.
- Phân tích các cấu trúc ngữ pháp để tìm các khái quát liên quan đến giới.

2. Định kiến ​​tôn giáo
Định kiến ​​tôn giáo đề cập đến các thái độ và định kiến ​​ngầm ẩn thường nằm dưới nhận thức có ý thức và có thể cản trở sự hợp tác liên tôn.
Đặc điểm:
- Thành kiến ​​tôn giáo có thể dựa trên những khác biệt về mặt tâm lý xã hội như địa vị, kinh thánh hoặc ảnh hưởng xuyên quốc gia.
- Xu hướng thiên vị nhóm, trong đó nhóm trong được miêu tả tích cực và nhóm ngoài được miêu tả tiêu cực.
- Sự gắn bó khác nhau với các văn bản tôn giáo có thể củng cố nhận thức về các nhóm khác biệt về mặt thần học.
Xác định điều này bằng cách:
- Kiểm tra những định kiến ​​ngầm.
- Sự khác biệt trong phản ứng với các thông điệp liên tôn dựa trên nội dung và nguồn gốc của thông điệp.
- Đo lường sự chậm trễ trong việc quy kết các thuộc tính tích cực hoặc tiêu cực cho một số nhóm tôn giáo nhất định.

3. Thành kiến ​​chủng tộc
Thành kiến ​​phân biệt chủng tộc có thể là ngầm (tương đối vô thức) hoặc rõ ràng (có ý thức). Cả thành kiến ​​ngầm và rõ ràng đều phổ biến và dẫn đến hậu quả tiêu cực mạnh mẽ.
Đặc điểm:
- Thành kiến ​​chủng tộc được củng cố bằng cách phân loại, đánh đồng, định kiến ​​và phân biệt đối xử.
- Thành kiến ​​có thể dẫn đến những phán đoán sai lệch và hành vi phân biệt đối xử, ngay cả khi không có ý định.
Nhận biết điều này thông qua:
- Sự hiện diện của các khuôn mẫu trong các tương tác xã hội hoặc nghề nghiệp.
- Các phán đoán hoặc hành động sai lệch trong các tình huống không chắc chắn hoặc áp lực về thời gian.
- Sự khác biệt trong cách đối xử với các cá nhân dựa trên các đặc điểm của nhóm.

4. Định kiến ​​về khuynh hướng tình dục
Định nghĩa:
Định kiến ​​về khuynh hướng tình dục liên quan đến sự phân biệt đối xử có ý thức hoặc vô thức đối với các cá nhân dựa trên sở thích tình dục của họ. Điều này liên quan đến các quyết định hoặc hành động gây bất lợi cho những cá nhân này, cho dù ở nơi làm việc, cơ sở giáo dục hay các bối cảnh xã hội khác.

Các đặc điểm:
- Sự phân biệt đối xử dựa trên các khuôn mẫu hoặc định kiến ​​đối với nhóm thiểu số tình dục.
- Sự đối xử không bình đẳng trong việc làm, thăng chức hoặc các quyết định liên quan đến công việc khác.
- Sự từ chối các quyền hoặc dịch vụ bình đẳng.
Nhận biết điều này bằng cách:
- So sánh cách đối xử với người đồng tính và người dị tính trong những điều kiện tương tự.
- Phân tích ngôn ngữ và hành động phản ánh ngầm hoặc rõ ràng sự định kiến.
- Áp dụng không cân xứng các quy tắc gây bất lợi cho một số nhóm nhất định.

5. Phân biệt đối xử theo độ tuổi
Phân biệt đối xử theo độ tuổi bao gồm bất kỳ hình thức đối xử bất bình đẳng hoặc bất lợi nào đối với một người dựa trên độ tuổi của họ, trừ khi được biện minh bởi các mục tiêu chính sách xã hội hợp pháp hoặc lý do khách quan. Nó bao gồm cả phân biệt đối xử trực tiếp và gián tiếp.
Đặc điểm:
- Phân biệt đối xử theo độ tuổi khác nhau ở việc áp dụng các giới hạn độ tuổi cụ thể trong các quy định về chính sách xã hội.
- Các bài kiểm tra về tính tương xứng đặc biệt có liên quan khi các giới hạn độ tuổi không bình đẳng.
Nhận ra điều này bằng cách:
- Gây bất lợi cho các nhóm thông qua các giới hạn độ tuổi được xác định một cách cứng nhắc mà không có mối liên hệ khách quan nào với các mục tiêu dự kiến.
- Các trường hợp giới hạn độ tuổi cản trở quyền tiếp cận cụ thể với các phúc lợi xã hội.
- Các chỉ số như giới hạn bảo hiểm thất nghiệp cho các nhóm tuổi.

6. Định kiến ​​quốc tịch
Định kiến ​​quốc tịch đề cập đến sự bóp méo có hệ thống trong đó các quốc gia hoặc dân số của họ được mô tả theo cách không chính xác, rập khuôn hoặc hạ thấp.
Đặc điểm:
- Ngôn ngữ rập khuôn hoặc hạ thấp đối với một số quốc tịch nhất định.
- Chủ đề tập trung vào xung đột quân sự hoặc bất ổn chính trị đối với một số quốc gia nhất định.
Nhận ra điều này bằng cách:
- Các chủ đề như bạo lực, khủng bố hoặc tham nhũng, được thể hiện quá mức liên quan đến một số quốc gia nhất định.

7. Định kiến ​​về khuyết tật
Định kiến ​​đối với người khuyết tật tự động đề cập đến Thái độ vô thức, chủ động dẫn đến việc người khuyết tật bị nhìn nhận hoặc đối xử tiêu cực. Những thái độ này dựa trên các khuôn mẫu và mối liên hệ xã hội thường dẫn đến hành vi phân biệt đối xử.
Đặc điểm:
- Sở thích ngầm tiêu cực đối với người không khuyết tật hơn người khuyết tật.
- Tự động liên tưởng khuyết tật với các thuật ngữ tiêu cực (ví dụ: xấu, bất tài).
- Xu hướng coi người khuyết tật là trẻ con hoặc kém năng lực.
Nhận ra điều này bằng cách:
- Phân tích mối liên hệ giữa các thuật ngữ như "khuyết tật" và "tiêu cực" trong văn bản.
- Xác định các cách diễn đạt tinh tế gợi ý sự thiếu năng lực hoặc tự chủ.
- Xem xét các giả định ngầm trong quá trình ra quyết định gây bất lợi cho người khuyết tật.

8. Định kiến ​​về ngoại hình
Sự hấp dẫn về ngoại hình là một phương pháp thường được sử dụng như một chỉ báo về các đặc điểm mong muốn. Các nghiên cứu cho thấy mọi người có nhiều khả năng gán các phẩm chất đạo đức cho những cá nhân hấp dẫn hơn là những cá nhân không hấp dẫn, một hiệu ứng mạnh hơn xu hướng liên tưởng những người hấp dẫn với các phẩm chất phi đạo đức tích cực. Điều này cho thấy sức hấp dẫn về mặt thể chất ảnh hưởng mạnh mẽ đến nhận thức về phẩm chất đạo đức.
Đặc điểm:
- Những cá nhân hấp dẫn có nhiều khả năng được coi là có đạo đức.
- Việc quy kết các phẩm chất đạo đức mạnh hơn việc quy kết các phẩm chất phi đạo đức.
- Nhận thức về phẩm chất đạo đức dựa trên các đánh giá nhanh chóng.
Phát hiện điều này bằng cách:
- Kiểm tra xem các phẩm chất đạo đức như sự trung thực hay đáng tin cậy có liên quan đến sức hấp dẫn về mặt thể chất hay không.
- Phân tích các đánh giá chiếu sự mong muốn xã hội không cân xứng lên những cá nhân hấp dẫn.
- So sánh các mô hình quy kết đạo đức và phi đạo đức đối với những cá nhân hấp dẫn so với những cá nhân không hấp dẫn.

9. Định kiến ​​về địa vị kinh tế xã hội:
Định kiến ​​về địa vị kinh tế xã hội mô tả những sự bóp méo có hệ thống xảy ra khi mọi người bị đối xử hoặc đánh giá không bình đẳng dựa trên vị thế kinh tế và xã hội của họ, điều này làm suy yếu các cơ hội bình đẳng.
Đặc điểm
- Sự bóp méo trong việc tiếp cận giáo dục và cơ hội việc làm.
- Nhận thức và cách đối xử khác nhau dựa trên thu nhập hoặc sự giàu có.
- Ảnh hưởng của nhận thức chủ quan về địa vị đối với các quyết định.
Nhận ra điều này bằng cách
- Phân tích sự khác biệt giữa các nhóm kinh tế xã hội về giáo dục, thu nhập hoặc tham gia chính trị.
- Xác định kết quả không bình đẳng mặc dù có khả năng hoặc nguồn lực tương đương.
- Quan sát sự di chuyển xã hội và các rào cản về mặt cấu trúc.

Sau đây là văn bản phán quyết:

{text}

KẾT THÚC VĂN BẢN PHÁN QUYẾT
Định dạng đầu ra:
Nếu phán quyết không chứa bất kỳ thành kiến ​​nào được đề cập, chỉ cần trả lời bằng cụm từ "Không có thành kiến" và không đưa ra lý do cho quyết định của bạn. Nếu không, hãy trả lời như sau:
Nếu bạn đã xác định được một loại thành kiến, hãy cấu trúc câu trả lời của bạn theo định dạng gồm ba phần riêng biệt để tôi có thể xử lý thêm.
Trình bày câu trả lời của bạn dưới dạng văn bản liên tục và theo định dạng sau:
Thành kiến ​​đã xác định: [Ở đây, bạn nêu loại thành kiến, tức là một trong những loại sau: "thành kiến ​​giới tính", "thành kiến ​​tôn giáo", "thành kiến ​​chủng tộc", "thành kiến ​​khuynh hướng tình dục", "phân biệt đối xử về tuổi tác", "thành kiến ​​quốc tịch", "thành kiến ​​khuyết tật", "thành kiến ​​ngoại hình" hoặc "thành kiến ​​địa vị kinh tế xã hội"]
Đoạn văn: "[Ở đây, bạn trích dẫn đoạn văn có liên quan từ văn bản phán quyết]"
Căn cứ: [Giải thích lý do tại sao đoạn văn này thể hiện thành kiến ​​này]
Nếu văn bản phán quyết chứa nhiều thành kiến, hãy trả lời nhiều lần theo định dạng này.
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

    bias_check_single_en(model="deepseek-r1:70b", run_id=9)
