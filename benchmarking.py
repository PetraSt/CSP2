import random
import string
import time
import duckdb
import psycopg2

base_tuples = 200 * 1000
repeat_insert = 8


def connect_postgres():
    # connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="test",
        user="postgres",
        password="root"
    )

    return conn


def run_benchmark_strings_duck(db, dbname, print_tuple_count, count_distinct=True):
    db.execute("""CREATE TABLE data(id INTEGER, dt INTEGER, shop TEXT, product TEXT, aa TEXT, bb INTEGER,
                customer TEXT);""")

    db.execute("begin transaction")
    for i in range(base_tuples):
        dt = random.randint(1600000000, 1600100000)  # random datetime (unix timestamp)
        shop = ''.join(random.choices(string.ascii_uppercase, k=1))  # 26 possibilities
        product = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        aa = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        customer = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        db.execute("INSERT INTO data(id, dt, shop, product, aa, bb, customer) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   [i, dt, shop, product, aa, 0, customer])

    db.commit()

    if print_tuple_count:
        tuple_count = db.execute('SELECT COUNT(*) FROM data').fetchall()[0][0]
        print("Total tuples: %d" % (tuple_count,))

    start = time.time()
    if not count_distinct:
        db.execute("SELECT product, COUNT(customer) count FROM data WHERE shop == 'A' GROUP BY product").fetchall()
    else:
        db.execute(
            "SELECT product, COUNT(DISTINCT customer) count FROM data WHERE shop == 'A' GROUP BY product").fetchall()
    print("%s: %.1f ms" % (dbname, (time.time() - start) * 1000))


def run_benchmark_integers_duck(db, dbname, print_tuple_count, count_distinct=True):
    db.execute("""CREATE TABLE data(id INTEGER, dt INTEGER, shop INTEGER, product INTEGER, aa INTEGER, bb INTEGER,
                        customer INTEGER);""")

    db.execute("begin transaction")
    for i in range(base_tuples):
        dt = random.randint(1600000000, 1600100000)  # random datetime (unix timestamp)
        shop = random.randint(0, 26)  # 26 possibilities
        product = random.randint(0, 676)  # 676 possibilities
        aa = random.randint(0, 676)  # 676 possibilities
        customer = random.randint(0, 676)  # 676 possibilities
    db.execute("INSERT INTO data(id, dt, shop, product, aa, bb, customer) VALUES (?, ?, ?, ?, ?, ?, ?)",
               [i, dt, shop, product, aa, 0, customer])

    db.commit()

    if print_tuple_count:
        tuple_count = db.execute('SELECT COUNT(*) FROM data').fetchall()[0][0]
        print("Total tuples: %d" % (tuple_count,))

    start = time.time()
    if not count_distinct:
        db.execute("SELECT product, COUNT(customer) count FROM data WHERE shop == 1 GROUP BY product").fetchall()
    else:
        db.execute(
            "SELECT product, COUNT(DISTINCT customer) count FROM data WHERE shop == 1 GROUP BY product").fetchall()
    print("%s: %.1f ms" % (dbname, (time.time() - start) * 1000))


def run_benchmark_strings_postgres(conn, dbname, print_tuple_count, count_distinct=True):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS data")
    cursor.execute("""CREATE TABLE data(id INTEGER, dt INTEGER, shop TEXT, product TEXT, aa TEXT, bb INTEGER,
                        customer TEXT);""")

    cursor.execute("begin transaction")
    for i in range(base_tuples):
        dt = random.randint(1600000000, 1600100000)  # random datetime (unix timestamp)
        shop = ''.join(random.choices(string.ascii_uppercase, k=1))  # 26 possibilities
        product = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        aa = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        customer = ''.join(random.choices(string.ascii_uppercase, k=2))  # 676 possibilities
        cursor.execute(
            "INSERT INTO data(id, dt, shop, product, aa, bb, customer) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            (i, dt, shop, product, aa, 0, customer))

    conn.commit()
    time.sleep(10)
    if print_tuple_count:
        cursor.execute("SELECT COUNT(*) FROM data;")
        tuple_count = cursor.fetchall()
        print("Total tuples: %d" % (tuple_count[0][0],))

    start = time.time()
    if not count_distinct:
        cursor.execute("SELECT product, COUNT(customer) count FROM data WHERE shop = 'A' GROUP BY product;")
        cursor.fetchall()
    else:
        cursor.execute("SELECT product, COUNT(DISTINCT customer) count FROM data WHERE shop = 'A' GROUP BY product;")
        cursor.fetchall()
    print("%s: %.1f ms" % (dbname, (time.time() - start) * 1000))


def run_benchmark_integers_postgres(conn, dbname, print_tuple_count, count_distinct=True):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS data")
    cursor.execute("""CREATE TABLE data(id INTEGER, dt INTEGER, shop INTEGER, product INTEGER, aa INTEGER, bb INTEGER,
                    customer INTEGER);""")

    cursor.execute("begin transaction")
    for i in range(base_tuples):
        dt = random.randint(1600000000, 1600100000)  # random datetime (unix timestamp)
        shop = random.randint(0, 26)  # 26 possibilities
        product = random.randint(0, 676)  # 676 possibilities
        aa = random.randint(0, 676)  # 676 possibilities
        customer = random.randint(0, 676)  # 676 possibilities

    cursor.execute("INSERT INTO data(id, dt, shop, product, aa, bb, customer) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                   (i, dt, shop, product, aa, 0, customer))

    conn.commit()
    time.sleep(10)

    if print_tuple_count:
        cursor.execute("SELECT COUNT(*) FROM data;")
        tuple_count = cursor.fetchall()
        print("Total tuples: %d" % (tuple_count[0][0],))

    start = time.time()
    if not count_distinct:
        cursor.execute("SELECT product, COUNT(customer) count FROM data WHERE shop = 1 GROUP BY product;")
        cursor.fetchall()
    else:
        cursor.execute("SELECT product, COUNT(DISTINCT customer) count FROM data WHERE shop = 1 GROUP BY product;")
        cursor.fetchall()
    print("%s: %.1f ms" % (dbname, (time.time() - start) * 1000))


duck_db = duckdb.connect('test1')
postgres_db = connect_postgres()

print("COUNT(DISTINCT) - VARCHAR")

run_benchmark_strings_duck(duck_db, 'DuckDB', True)
run_benchmark_strings_postgres(postgres_db, 'postgres', True)

duck_db = duckdb.connect('test2')

print("COUNT(DISTINCT) - INTEGER")

run_benchmark_integers_duck(duck_db, 'DuckDB', True)
run_benchmark_integers_postgres(postgres_db, 'postgres', True)

print("COUNT - VARCHAR")

duck_db = duckdb.connect('test3')

run_benchmark_strings_duck(duck_db, 'DuckDB', True, False)
run_benchmark_strings_postgres(postgres_db, 'postgres', True, False)

duck_db = duckdb.connect('test4')

print("COUNT - INTEGER")

run_benchmark_integers_duck(duck_db, 'DuckDB', True, False)
run_benchmark_integers_postgres(postgres_db, 'postgres', True, False)
