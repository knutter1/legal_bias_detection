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
RUN_ID = 7
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
        "run_id": RUN_ID
    }


def bias_check_single(model, run_id):
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
                {"ollama_responses.run_id": RUN_ID, "language": "Vietnamese"})

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
