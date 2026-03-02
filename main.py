import time
from grabfood_models import Restaurant
from grabfood_pages_parser import load_files,parser
from grabfood_database import send_to_db


start_time=time.time()

file_name="grab_food_pages"
file_data=load_files(file_name)
extracted_data = parser(file_data)

validated_data=[]
failed_record=0

for record in extracted_data:
    try:
        validated_record = Restaurant(**record)
        validated_data.append(validated_record)
    except Exception as e:
        failed_record += 1
        print("Validation error:", e)


print(f"Valid data: {len(validated_data)}")
print(f"Invalid data: {failed_record}")

if validated_data:
    try:
        send_to_db([record.model_dump() for record in validated_data])
        print("data inserted successfully")
    except Exception as e:
        print("database error", e)

else:
    print("No valid data to insert")

end_time=time.time()
total_time=end_time-start_time
print(total_time)
