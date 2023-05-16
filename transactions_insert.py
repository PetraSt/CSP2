import csv
import threading
import time
import duckdb
import psycopg2


def duck_client_transactions(x, book_id):
    query = """INSERT INTO book (title, book_id, isbn13, language_id, num_pages, publication_date, publisher_id) 
        VALUES ('The Metamorphosis', {}, '8987059752', 2, 276, '1996-09-01', 1010);"""
    cursor = duckdb.connect(database="bookstore" + str(x * 10000) + ".db")
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(book_id)
        book_id += 1
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def postgres_client_join(x, book_id):
    query = """INSERT INTO book (title, book_id, isbn13, language_id, num_pages, publication_date, publisher_id) 
            VALUES ('The Metamorphosis', {}, '8987059752', 2, 276, '1996-09-01', 1010);"""

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
        book_id += 1
        cursor.execute(final_query)
    conn.commit()
    tuple_count = cursor.fetchall()

def duck_transactions(num, times):
    # query = """INSERT INTO book (title, book_id, isbn13, language_id, num_pages, publication_date, publisher_id)
    # VALUES ('The Metamorphosis', {}, '8987059752', 2, 276, '1996-09-01', 1010);"""
    #
    cursor = duckdb.connect(database="bookstore"+str(num)+"0000.db")
    cursor.execute("SELECT MAX(book_id) FROM book;")
    cursor.commit()
    book_id = cursor.fetchall()[0][0] + 1
    # start = time.time()
    # cursor.execute("begin transaction")
    # for i in range(0, times):
    #     book_id += 1
    #     final_query = query.format(book_id)
    #     cursor.execute(final_query)
    # cursor.commit()
    # cursor.close()
    # # tuple_count = cursor.fetchall()
    # return (time.time() - start) * 1000
    # Create a list of threads
    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        thread = threading.Thread(target=duck_client_transactions, args=(num, book_id,))
        book_id += 100
        threads.append(thread)
    return (time.time() - start) * 1000


def postgres_transactions(num, times):
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore"+str(num)+"0000",
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(book_id) FROM book;")
    conn.commit()
    book_id = cursor.fetchall()[0][0] + 1
    # start = time.time()
    # cursor.execute("begin transaction")
    # for i in range(0, times):
    #     book_id += 1
    #     final_query = query.format(book_id)
    #     cursor.execute(final_query)
    # conn.commit()
    # # tuple_count = cursor.fetchall()
    # cursor.close()
    # conn.close()
    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        thread = threading.Thread(target=postgres_client_join, args=(num, book_id,))
        book_id += 100
        threads.append(thread)
    return (time.time() - start) * 1000


def main(k, insert_times_duck, insert_times_postgres):
    for key in insert_times_duck:
        return_time = duck_transactions(k, key)
        insert_times_duck[key].append(return_time)
    for key in insert_times_postgres:
        return_time = postgres_transactions(k, key)
        insert_times_postgres[key].append(return_time)


if __name__ == '__main__':
    for z in range(1, 11):
        keys = [1, 5, 10, 15, 20]
        insert_time_duck = {key: [] for key in keys}
        insert_time_postgres = {key: [] for key in keys}
        num_iterations = 10
        for j in range(0, 10):
            main(z, insert_time_duck, insert_time_postgres)
        with open('output3_' + str(z) + '0000_insert.csv', mode='w') as file:
            writer = csv.writer(file)
            for key in insert_time_duck:
                writer.writerow(['duckdb', key, sum(insert_time_duck[key]) / num_iterations])
            for key in insert_time_postgres:
                writer.writerow(['postgres', key, sum(insert_time_postgres[key]) / num_iterations])