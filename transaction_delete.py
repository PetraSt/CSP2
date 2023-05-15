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


def main():
    with open('output20000_delete.csv', mode='w') as file:
        writer = csv.writer(file)
        return_time = duck_transactions(100)
        writer.writerow(['duckdb', 100, return_time])
        return_time = duck_transactions(500)
        writer.writerow(['duckdb', 500, return_time])
        return_time = duck_transactions(1000)
        writer.writerow(['duckdb', 1000, return_time])
        return_time = duck_transactions(1500)
        writer.writerow(['duckdb', 1500, return_time])
        return_time = duck_transactions(2000)
        writer.writerow(['duckdb', 2000, return_time])
        return_time = postgres_transactions(100)
        writer.writerow(['postgres', 100, return_time])
        return_time = postgres_transactions(500)
        writer.writerow(['postgres', 500, return_time])
        return_time = postgres_transactions(1000)
        writer.writerow(['postgres', 1000, return_time])
        return_time = postgres_transactions(1500)
        writer.writerow(['postgres', 1500, return_time])
        return_time = postgres_transactions(2000)
        writer.writerow(['postgres', 2000, return_time])


if __name__ == '__main__':
    main()
