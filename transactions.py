import threading
import time
import duckdb
import psycopg2

common_words = ['the', 'of', 'and', 'a', 'in', 'to', '#1)', '#2)', 'on', '#3)', 'for', '(the', '&', 'from', '/', 'life', 'stories', 'vol.', 'other', 'guide', 'history', 'an', '#4)', 'world', 'with', 'volume', 'love', 'book', 'how', 'new', 'story', 'de', 'complete', 'my', 'war', 'at', 'you', 'time', '#5)', 'i', 'american', 'tales', '#6)', 'your', 'house', 'is', 'la', 'one', 'man', 'black', 'little', 'death', 'art', 'trilogy', 'last', 'by', 'night', 'dark', '1', 'great', 'lost', 'short', 'el', 'star', 'that', 'his', '#7)', 'three', 'best', 'who', 'poems', 'what', 'america', 'lord', 'first', 'like', 'king', 'essays', 'all', 'girl', 'no', 'adventures', 'secret', '#8)', 'women', 'writings', 'philosophy', 'chronicles', 'it', 'family', 'as', 'children', 'murder', 'mystery', '1:', 'selected', 'true', 'collected', 'white', 'journey']


def duck_client_join():
    query = """SELECT publisher.publisher_name
            FROM publisher
            JOIN book ON book.publisher_id = publisher.publisher_id
            GROUP BY publisher.publisher_id, publisher.publisher_name
            HAVING COUNT(DISTINCT book.language_id) >= {};"""
    cursor = duckdb.connect(database="bookstore.db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format((i % 27)+1)
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_client_integer():
    query = """SELECT count(*), avg(num_pages) FROM book
            WHERE num_pages>={} AND num_pages<{};"""

    cursor = duckdb.connect(database="bookstore.db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0,7000,70):
        final_query = query.format(i,i+70)
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_client_string():
    query = """SELECT count(*), avg(num_pages) FROM book
                WHERE title LIKE '%{}%';"""

    cursor = duckdb.connect(database="bookstore.db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(common_words[i])
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_transactions(times, query_type):
    # Create a list of threads
    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        if query_type == 1:
            thread = threading.Thread(target=duck_client_join())
            threads.append(thread)
        elif query_type == 2:
            thread = threading.Thread(target=duck_client_integer())
            threads.append(thread)
        else:
            thread = threading.Thread(target=duck_client_string())
            threads.append(thread)

    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("duckdb_%d_%d: %.1f ms" % (times, query_type, (time.time() - start) * 1000))


def postgres_client_join():
    query = """SELECT publisher.publisher_name
                FROM publisher
                JOIN book ON book.publisher_id = publisher.publisher_id
                GROUP BY publisher.publisher_id, publisher.publisher_name
                HAVING COUNT(DISTINCT book.language_id) >= {};"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore",
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format((i % 27) + 1)
        cursor.execute(final_query)
    conn.commit()
    tuple_count = cursor.fetchall()


def postgres_client_integer():
    query = """SELECT count(*), avg(num_pages) FROM book
            WHERE num_pages>={} AND num_pages<{};"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore",
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("begin transaction")
    for i in range(0, 7000, 70):
        final_query = query.format(i, i + 70)
        cursor.execute(final_query)
    conn.commit()
    tuple_count = cursor.fetchall()


def postgres_client_string():
    query = """SELECT count(*), avg(num_pages) FROM book
                WHERE title LIKE '%{}%';"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore",
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(common_words[i])
        cursor.execute(final_query)
    conn.commit()
    tuple_count = cursor.fetchall()


def postgres_transactions(times, query_type):
    # Create a list of threads

    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        if query_type == 1:
            thread = threading.Thread(target=postgres_client_join())
            threads.append(thread)
        elif query_type == 2:
            thread = threading.Thread(target=postgres_client_integer())
            threads.append(thread)
        else:
            thread = threading.Thread(target=postgres_client_string())
            threads.append(thread)

    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("postgres_%d_%d: %.1f ms" % (times, query_type, (time.time() - start) * 1000))


def main():
    duck_transactions(1, 1)
    postgres_transactions(1, 1)
    duck_transactions(5, 1)
    postgres_transactions(5, 1)
    duck_transactions(10, 1)
    postgres_transactions(10, 1)
    duck_transactions(15, 1)
    postgres_transactions(15, 1)
    duck_transactions(20, 1)
    postgres_transactions(20, 1)

    duck_transactions(1, 2)
    postgres_transactions(1, 2)
    duck_transactions(5, 2)
    postgres_transactions(5, 2)
    duck_transactions(10, 2)
    postgres_transactions(10, 2)
    duck_transactions(15, 2)
    postgres_transactions(15, 2)
    duck_transactions(20, 2)
    postgres_transactions(20, 2)

    duck_transactions(1, 3)
    postgres_transactions(1, 3)
    duck_transactions(5, 3)
    postgres_transactions(5, 3)
    duck_transactions(15, 3)
    postgres_transactions(10, 3)
    duck_transactions(10, 3)
    postgres_transactions(15, 3)
    duck_transactions(20, 3)
    postgres_transactions(20, 3)

if __name__ == '__main__':
    main()
