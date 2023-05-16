import csv
import threading
import time
import duckdb
import psycopg2


def duck_transactions(times):
    query = """DELETE FROM book WHERE book_id = {};"""

    cursor = duckdb.connect(database="bookstore.db")
    cursor.execute("SELECT MAX(book_id) FROM book;")
    cursor.commit()
    book_id = cursor.fetchall()[0][0]
    start = time.time()
    cursor.execute("begin transaction")
    for i in range(0, times):
        final_query = query.format(book_id)
        cursor.execute(final_query)
        book_id -= 1
        if book_id == 0:
            break
    cursor.commit()
    cursor.close()
    # tuple_count = cursor.fetchall()
    return (time.time() - start) * 1000


def postgres_transactions(times):
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
    start = time.time()
    cursor.execute("begin transaction")
    for i in range(0, times):
        final_query = query.format(book_id)
        cursor.execute(final_query)
        book_id -= 1
        if book_id == 0:
            break
    conn.commit()
    # tuple_count = cursor.fetchall()
    cursor.close()
    conn.close()
    return (time.time() - start) * 1000


def main(delete_times_duck, delete_times_postgres):
    for key in delete_times_duck:
        return_time = duck_transactions(key)
        delete_times_duck[key].append(return_time)
    for key in delete_times_postgres:
        return_time = postgres_transactions(key)
        delete_times_postgres[key].append(return_time)


if __name__ == '__main__':
    for z in range(1, 11):
        keys = [100, 500, 1000, 1500, 2000]
        delete_time_duck = {key: [] for key in keys}
        delete_time_postgres = {key: [] for key in keys}
        num_iterations = 10
        for j in range(0, 10):
            main(delete_time_duck, delete_time_postgres)
        with open('output3_' + str(z) + '0000_delete.csv', mode='w') as file:
            writer = csv.writer(file)
            for key in delete_time_duck:
                writer.writerow(['duckdb', key, sum(delete_time_duck[key]) / num_iterations])
            for key in delete_time_postgres:
                writer.writerow(['postgres', key, sum(delete_time_postgres[key]) / num_iterations])
