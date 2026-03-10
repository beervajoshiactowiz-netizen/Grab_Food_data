import time
import sys
import threading
import logging
from grabfood_models import Restaurant
from grabfood_pages_parser import parser
import gzip, json, os
from grabfood_database import (
    create_connection,
    create_tables,
    send_to_db
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("grabfood.log"),   # saves to file
    ]
)
logger = logging.getLogger(__name__)

query_logger = logging.getLogger("query_logger")
query_logger.setLevel(logging.INFO)
query_handler = logging.FileHandler("query.log", encoding='utf-8')
query_handler.setFormatter(logging.Formatter("%(message)s"))
query_logger.addHandler(query_handler)
query_logger.propagate = False

create_logger = logging.getLogger("create_logger")
create_logger.setLevel(logging.INFO)
create_handler = logging.FileHandler("create_query.log",encoding='utf-8')
create_handler.setFormatter(logging.Formatter("%(message)s"))
create_logger.addHandler(create_handler)
create_logger.propagate = False

folder_name="PDP"
total_inserted=0
total_failed=0
lock=threading.Lock()


def main(thread_id, start_index, end_index):
    global total_failed,total_inserted
    failed_record=0

    conn = create_connection()     #calling the connection function
    cursor = conn.cursor()    #cursor object
    create_tables(cursor)

    all_files = sorted([f for f in os.listdir(folder_name) if f.endswith(".gz")])
    selected_files = all_files[start_index:end_index]


    validated_data = []
    for file in selected_files:
        fullpath = os.path.join(folder_name, file)

        try:
            with gzip.open(fullpath, 'rt', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, dict):
                pages = [data]
            elif isinstance(data, list):
                pages = data
            else:
                continue
            #calling parser function to parse each file
            extracted_data = parser(pages)

            for record in extracted_data:
                try:
                    validated_record = Restaurant(**record)
                    validated_data.append(validated_record.model_dump())
                except Exception:
                    failed_record += 1

        except Exception as e:
            logger.error(f"[Thread-{thread_id}] File error: {file} -> {e}")

    if validated_data:
        send_to_db(validated_data,cursor,conn)
        logger.info(f"[Thread-{thread_id}] Inserted: {len(validated_data)} | Failed: {failed_record}")



    cursor.close()
    conn.close()

    with lock:
        total_inserted += len(validated_data)
        total_failed += failed_record
        validated_data.clear()


def total(total_files,parts):

    range_of_files=int(total_files/parts)
    threads=[]

    for i, start in enumerate(range(0, total_files, range_of_files), 1):
        end = min(start + range_of_files, total_files)
        logger.info(f"[Thread-{i}] Starting -> files {start} to {end}")
        t = threading.Thread(target=main, args=(i, start, end))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()



if __name__=="__main__":
    start_time = time.time()
    total(60000,6)
    logger.info(f"Time: {time.time() - start_time:.2f} sec")
