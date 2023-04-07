import psycopg2

DATABASE = "TPC-H"
HOST = "localhost"
USER = "postgres"
PASSWORD = "password"
PORT = 5432

conn = psycopg2.connect(database=DATABASE,
                        host=HOST,
                        user=USER,
                        password=PASSWORD,
                        port=PORT)
cursor = conn.cursor()

def get_query_plan(query: str):
    cursor.execute("EXPLAIN (FORMAT JSON, VERBOSE) " + query)
    res = cursor.fetchone()
    if not res or not res[0]:
        print("no plan returned")
        return None

    return res[0][0]
    # print(res[0][0])