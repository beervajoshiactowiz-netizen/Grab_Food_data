import json
import mysql.connector

# batches
def make_batches(data_list, batch_size=1000):
    for i in range(0, len(data_list), batch_size):
        yield data_list[i : i + batch_size]


def send_to_db(data):
    conn=None
    cursor=None
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="grabfood_db"
        )
        cursor = conn.cursor()

        #Restaurant data table creation
        create_restaurant_query = """
        CREATE TABLE IF NOT EXISTS grab_restaurant (
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
            Offers JSON
        );
        """
        cursor.execute(create_restaurant_query)

        insert_restaurant = """
        INSERT INTO grab_restaurant
        (merchant_id, name, cuisine, timingEveryday, distance,
         ETA, rating, DeliveryBy,DeliveryOption, VoteCount, Tips, BuisinessType,Offers)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
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
        Offers= VALUES(Offers)
        """
        #Menu items data table creation
        create_menu_query = """
        CREATE TABLE IF NOT EXISTS menu (
            id bigint auto_increment PRIMARY KEY,
            item_id VARCHAR(100) unique,
            merchant_id VARCHAR(100),
            category_name VARCHAR(100),
            name VARCHAR(255),
            description TEXT,
            price FLOAT,
            available BOOLEAN,
            images JSON,
            FOREIGN KEY (merchant_id) REFERENCES grab_restaurant(merchant_id) 
        );
        """
        cursor.execute(create_menu_query)

        menu_insert = """
        INSERT INTO menu
        (item_id, merchant_id, category_name, name,
         description, price, available, images)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        item_id=VALUES(item_id)
        """

        restaurant_list=[]
        menu_list=[]
        for restaurant in data:
            merchantId = restaurant.get("merchant_id")
            if not merchantId:
                continue

            restaurant_list.append( (
                merchantId,
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
                json.dumps(restaurant.get("Offers", []))
            ))

            # Nested loop for menu
            for category in restaurant.get("menu", []):
                category_name = category.get("category_name")

                for item in category.get("items", []):
                    menu_list.append( (
                        item.get("item_id"),
                        restaurant.get("merchant_id"),
                        category_name,
                        item.get("name"),
                        item.get("description"),
                        item.get("price_display"),
                        item.get("available"),
                        json.dumps(item.get("images", []))
                    ))

        for batch in make_batches(restaurant_list, 1000):
            cursor.executemany(insert_restaurant, batch)
            conn.commit()
        print(f"Processed {len(restaurant_list)} restaurants.")

        # Executing Batches for Menu Items
        for batch in make_batches(menu_list, 1000):
            cursor.executemany(menu_insert, batch)
            conn.commit()
        print(f"Processed {len(menu_list)} menu items.")

    except mysql.connector.Error as err:
        if conn:
            conn.rollback()
        print("Error occurred: ",err)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


