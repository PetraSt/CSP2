import csv
import threading
import time
import duckdb
import psycopg2


def duck_client_transactions(x, book_id):
    query = """DELETE FROM book WHERE book_id = {};"""
    cursor = duckdb.connect(database="bookstore" + str(x * 10000) + ".db")
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(book_id)
        book_id -= 1
        if book_id == 0:
            break
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def postgres_client_join(x, book_id):
    query = """DELETE FROM book WHERE book_id = {};"""

    db = "bookstore" + str(x * 10000)
    conn = psycopg2.connect(
        host="localhost",
        database=db,
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(book_id)
        book_id -= 1
        if book_id == 0:
            break
        cursor.execute(final_query)
    conn.commit()
    tuple_count = cursor.fetchall()


def duck_transactions(num, times):
    query = """DELETE FROM book WHERE book_id = {};"""

    cursor = duckdb.connect(database="bookstore" + str(num) + "0000.db")
    cursor.execute("SELECT MAX(book_id) FROM book;")
    cursor.commit()
    book_id = cursor.fetchall()[0][0]
    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        thread = threading.Thread(target=duck_client_transactions, args=(num, book_id,))
        book_id -= 100
        if book_id == 0:
            break
        threads.append(thread)
    return (time.time() - start) * 1000


def postgres_transactions(num, times):
    query = """DELETE FROM book WHERE book_id = {};"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore",
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(book_id) FROM book;")
    conn.commit()
    book_id = cursor.fetchall()[0][0]
    threads = []
    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        thread = threading.Thread(target=postgres_client_join, args=(num, book_id,))
        book_id -= 100
        if book_id == 0:
            break
        threads.append(thread)
    return (time.time() - start) * 1000


def main(k, delete_times_duck, delete_times_postgres):
    for key in delete_times_duck:
        return_time = duck_transactions(k, key)
        delete_times_duck[key].append(return_time)
    for key in delete_times_postgres:
        return_time = postgres_transactions(k, key)
        delete_times_postgres[key].append(return_time)


if __name__ == '__main__':
    for z in range(1, 11):
        keys = [1, 5, 10, 15, 20]
        delete_time_duck = {key: [] for key in keys}
        delete_time_postgres = {key: [] for key in keys}
        num_iterations = 10
        for j in range(0, 10):
            main(z, delete_time_duck, delete_time_postgres)
        with open('output3_' + str(z) + '0000_delete.csv', mode='w') as file:
            writer = csv.writer(file)
            for key in delete_time_duck:
                writer.writerow(['duckdb', key, sum(delete_time_duck[key]) / num_iterations])
            for key in delete_time_postgres:
                writer.writerow(['postgres', key, sum(delete_time_postgres[key]) / num_iterations])
