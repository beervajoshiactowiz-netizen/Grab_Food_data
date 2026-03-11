import time
import threading
import gzip
import json
import os
import logging
import logging.handlers

from grabfood_models import Restaurant
from parser_2 import parser
from database_2 import create_connection, create_tables, send_to_db

logging.basicConfig(
    filename='grabfood_processing_2.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
os.makedirs('logs', exist_ok=True)
query_logger = logging.getLogger('query_logger')
query_logger.setLevel(logging.INFO)
if not query_logger.handlers:
    query_handler = logging.handlers.RotatingFileHandler('logs/query.log', maxBytes=100*1024*1024, backupCount=30,encoding='utf-8')
    query_handler.setFormatter(logging.Formatter('%(message)s'))
    query_logger.addHandler(query_handler)
query_logger.propagate = False

folder_name = r"C:\Users\beerva.joshi\PycharmProjects\grabfood\PDP"

total_inserted = 0
total_failed = 0

lock = threading.Lock()


def main(thread_id, selected_files):

    global total_failed, total_inserted

    failed_record = 0
    inserted_count = 0

    conn = create_connection()
    cursor = conn.cursor()

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

            extracted_data = parser(pages)

            for record in extracted_data:
                try:
                    validated_record = Restaurant(**record)
                    validated_data.append(validated_record.model_dump())

                except Exception:
                    failed_record += 1

        except Exception as e:
            logger.error("File error: %s, Error: %s", file, e)

        # Insert every 2000 records to avoid huge memory list
        if len(validated_data) >= 2000:
            send_to_db(validated_data, cursor, conn)
            inserted_count += len(validated_data)
            validated_data.clear()

    # Insert remaining records
    if validated_data:
        send_to_db(validated_data, cursor, conn)
        inserted_count += len(validated_data)

    cursor.close()
    conn.close()

    logger.info(f"[Thread-{thread_id}] Inserted {inserted_count}")

    with lock:
        total_inserted += inserted_count
        total_failed += failed_record


def total(total_files, parts):

    global folder_name

    threads = []

    # READ FILES ONLY ONCE
    all_files = sorted([f for f in os.listdir(folder_name) if f.endswith(".gz")])

    chunk_size = total_files // parts

    for i in range(parts):

        start = i * chunk_size

        if i == parts - 1:
            end = total_files
        else:
            end = start + chunk_size

        selected_files = all_files[start:end]

        logger.info(f"[Thread-{i+1}] Starting -> files {start} to {end}")

        t = threading.Thread(target=main, args=(i + 1, selected_files))

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    logger.info(f"Total Inserted : {total_inserted}")
    logger.info(f"Total Failed   : {total_failed}")


if __name__ == "__main__":

    start_time = time.time()

    # CREATE TABLES ONLY ONCE
    conn = create_connection()
    cursor = conn.cursor()
    create_tables(cursor)
    cursor.close()
    conn.close()

    total(60000, 6)

    logger.info(f"Total Time : {time.time() - start_time:.2f} sec")
