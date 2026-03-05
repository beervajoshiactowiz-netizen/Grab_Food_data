import json
import mysql.connector

# batches
def make_batches(data_list, batch_size=2000):
    for i in range(0, len(data_list), batch_size):
        yield data_list[i : i + batch_size]

def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="grabfood_db"
    )

def create_tables(cursor):
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS pdp_data (
                merchant_id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(255),
                cuisine VARCHAR(100),
                timingEveryday VARCHAR(100),
                distance FLOAT,
                ETA INT,
                rating FLOAT,
                DeliveryBy VARCHAR(100),
                DeliveryOption JSON,
                VoteCount INT,
                Tips JSON,
                BuisinessType VARCHAR(50),
                Offers JSON,
                menu JSON
            );
            """)
def send_to_db(data,cursor,conn):
        insert_restaurant = """
        INSERT INTO pdp_data
        (merchant_id, name, cuisine, timingEveryday, distance,
         ETA, rating, DeliveryBy,DeliveryOption, VoteCount, Tips, BuisinessType,Offers,menu)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        merchant_id=VALUES(merchant_id),
        name=VALUES(name),
        cuisine=VALUES(cuisine),
        timingEveryday=VALUES(timingEveryday),
        distance=VALUES(distance),
        ETA=VALUES(ETA),
        rating=VALUES(rating),
        DeliveryBy=VALUES(DeliveryBy),
        DeliveryOption=VALUES(DeliveryOption),
        VoteCount=VALUES(VoteCount),
        Tips=VALUES(Tips),
        BuisinessType=VALUES(BuisinessType),
        Offers= VALUES(Offers),
        menu=VALUES(menu)
        """


        restaurant_list=[]

        for restaurant in data:
            m_id = restaurant.get("merchant_id")
            if not m_id:
                continue

            restaurant_list.append( (
                m_id,
                restaurant.get("name"),
                restaurant.get("cuisine"),
                restaurant.get("timingEveryday"),
                restaurant.get("distance"),
                restaurant.get("ETA"),
                restaurant.get("rating"),
                restaurant.get("DeliveryBy"),
                json.dumps(restaurant.get("DeliveryOption", [])),
                restaurant.get("VoteCount"),
                json.dumps(restaurant.get("Tips", [])),
                restaurant.get("BuisinessType"),
                json.dumps(restaurant.get("Offers", [])),
                json.dumps(restaurant.get("menu",[]))
            ))

        for batch in make_batches(restaurant_list, 2000):
            cursor.executemany(insert_restaurant, batch)
            conn.commit()

        print(f"Processed {len(restaurant_list)} restaurants.")
