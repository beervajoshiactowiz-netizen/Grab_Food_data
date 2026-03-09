import time
import sys
import threading
from grabfood_models import Restaurant
from grabfood_pages_parser import parser
import gzip, json, os
from grabfood_database import (
    create_connection,
    create_tables,
    send_to_db
)


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
            print("File error:", file, e)

    if validated_data:
        send_to_db(validated_data,cursor,conn)
        print(f"[Thread-{thread_id}] Inserted chunk with {len(validated_data)} restaurants")
        validated_data.clear()


    cursor.close()
    conn.close()

    print("Failed Records:", failed_record)
    with lock:
        total_inserted += len(validated_data)
        total_failed += failed_record


def total(total_files,parts):

    range_of_files=int(total_files/parts)
    threads=[]

    for i, start in enumerate(range(0, total_files, range_of_files), 1):
        end = min(start + range_of_files, total_files)
        print(f"[Thread-{i}] Starting -> files {start} to {end}")
        t = threading.Thread(target=main, args=(i, start, end))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"Total Failed  : {total_failed}")


if __name__=="__main__":
    start_time = time.time()
    total(60000,10)
    print(f"Total Time    : {time.time() - start_time:.2f} sec")
