import time
import sys
from grabfood_models import Restaurant
from grabfood_pages_parser import parser
import gzip, json, os
from grabfood_database import (
    create_connection,
    create_tables,
    send_to_db
)

start_time=time.time()
folder_name="PDP"

failed_record=0

conn = create_connection()     #calling the connection function
cursor = conn.cursor()    #cursor object
create_tables(cursor)

start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
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
    print(f"Inserted chunk with {len(validated_data)} restaurants")
    validated_data.clear()


cursor.close()
conn.close()

print("Failed Records:", failed_record)
print("Total Time:", time.time() - start_time)
