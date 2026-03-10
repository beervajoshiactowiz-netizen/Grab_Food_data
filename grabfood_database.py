import json
import mysql.connector
import logging, threading

logger       = logging.getLogger(__name__)
query_logger = logging.getLogger("query_logger")
create_logger = logging.getLogger("create_logger")

create_query = """
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
            """


def format_value(val):
    if val is None:
        return "NULL"
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            val    = json.dumps(parsed, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            pass
        val = val.replace("\\", "\\\\")  # escape backslashes first
        val = val.replace("'",  "''")    # escape single quotes
        return f"'{val}'"
    return str(val)


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


_create_logged = False
_create_lock   = threading.Lock()

def create_tables(cursor):
    global _create_logged
    cursor.execute(create_query)

    with _create_lock:
        if not _create_logged:
            create_logger.info(create_query.strip())
            _create_logged = True


def send_to_db(data, cursor, conn):
    if not data:
        return

    restaurant_list = []

    for restaurant in data:
        m_id = restaurant.get("merchant_id")
        if not m_id:
            continue

        restaurant_list.append((
            m_id,
            restaurant.get("name"),
            restaurant.get("cuisine"),
            restaurant.get("timingEveryday"),
            restaurant.get("distance"),
            restaurant.get("ETA"),
            restaurant.get("rating"),
            restaurant.get("DeliveryBy"),
            json.dumps(restaurant.get("DeliveryOption", []), ensure_ascii=False),
            restaurant.get("VoteCount"),
            json.dumps(restaurant.get("Tips", []),           ensure_ascii=False),
            restaurant.get("BuisinessType"),
            json.dumps(restaurant.get("Offers", []),         ensure_ascii=False),
            json.dumps(restaurant.get("menu", []),           ensure_ascii=False)
        ))

    if not restaurant_list:
        return

    cols      = "merchant_id, name, cuisine, timingEveryday, distance, ETA, rating, DeliveryBy, DeliveryOption, VoteCount, Tips, BuisinessType, Offers, menu"
    col_list  = cols.split(", ")
    vals      = ", ".join(["%s"] * len(col_list))
    update    = ", ".join([f"{c}=VALUES({c})" for c in col_list])

    insert_q  = f"INSERT INTO pdp_data ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {update}"


    for batch in make_batches(restaurant_list, 2000):
        cursor.executemany(insert_q, batch)
        conn.commit()

        for row in batch:
            formatted_vals = ", ".join(format_value(v) for v in row)
            sql = f"INSERT INTO pdp_data ({cols}) VALUES ({formatted_vals}) ON DUPLICATE KEY UPDATE {update};"
            query_logger.info(sql)

    logger.info(f"Processed {len(restaurant_list)} restaurants.")
