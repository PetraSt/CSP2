import csv
import threading
import time
import duckdb
import psycopg2

common_words = ['the', 'of', 'and', 'a', 'in', 'to', '#1)', '#2)', 'on', '#3)', 'for', '(the', '&', 'from', '/', 'life',
                'stories', 'vol.', 'other', 'guide', 'history', 'an', '#4)', 'world', 'with', 'volume', 'love', 'book',
                'how', 'new', 'story', 'de', 'complete', 'my', 'war', 'at', 'you', 'time', '#5)', 'i', 'american',
                'tales', '#6)', 'your', 'house', 'is', 'la', 'one', 'man', 'black', 'little', 'death', 'art', 'trilogy',
                'last', 'by', 'night', 'dark', '1', 'great', 'lost', 'short', 'el', 'star', 'that', 'his', '#7)',
                'three', 'best', 'who', 'poems', 'what', 'america', 'lord', 'first', 'like', 'king', 'essays', 'all',
                'girl', 'no', 'adventures', 'secret', '#8)', 'women', 'writings', 'philosophy', 'chronicles', 'it',
                'family', 'as', 'children', 'murder', 'mystery', '1:', 'selected', 'true', 'collected', 'white',
                'journey']


def duck_client_join(x):
    query = """SELECT publisher.publisher_name
            FROM publisher
            JOIN book ON book.publisher_id = publisher.publisher_id
            GROUP BY publisher.publisher_id, publisher.publisher_name
            HAVING COUNT(DISTINCT book.language_id) >= {};"""
    cursor = duckdb.connect(database="bookstore"+str(x*10000)+".db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format((i % 27) + 1)
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_client_integer(x):
    query = """SELECT count(*), avg(num_pages) FROM book
            WHERE num_pages>={} AND num_pages<{};"""

    cursor = duckdb.connect(database="bookstore"+str(x*10000)+".db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0, 7000, 70):
        final_query = query.format(i, i + 70)
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_client_string(x):
    query = """SELECT count(*), avg(num_pages) FROM book
                WHERE title LIKE '%{}%';"""

    cursor = duckdb.connect(database="bookstore"+str(x*10000)+".db", read_only=True)
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(common_words[i])
        cursor.execute(final_query)
    cursor.commit()
    tuple_count = cursor.fetchall()


def duck_transactions(times, query_type, x):
    # Create a list of threads
    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        if query_type == 1:
            thread = threading.Thread(target=duck_client_join, args=(x))
            threads.append(thread)
        elif query_type == 2:
            thread = threading.Thread(target=duck_client_intege, args=(x))
            threads.append(thread)
        else:
            thread = threading.Thread(target=duck_client_string, args=(x))
            threads.append(thread)

    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    return (time.time() - start) * 1000


def postgres_client_join(x):
    query = """SELECT publisher.publisher_name
                FROM publisher
                JOIN book ON book.publisher_id = publisher.publisher_id
                GROUP BY publisher.publisher_id, publisher.publisher_name
                HAVING COUNT(DISTINCT book.language_id) >= {};"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore"+str(x*10000),
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


def postgres_client_integer(x):
    query = """SELECT count(*), avg(num_pages) FROM book
            WHERE num_pages>={} AND num_pages<{};"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore"+str(x*10000),
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


def postgres_client_string(x):
    query = """SELECT count(*), avg(num_pages) FROM book
                WHERE title LIKE '%{}%';"""
    conn = psycopg2.connect(
        host="localhost",
        database="bookstore"+str(x*10000),
        user="postgres",
        password="root"
    )

    cursor = conn.cursor()
    cursor.execute("begin transaction")
    for i in range(0, 100):
        final_query = query.format(common_words[i])
        cursor.execute(final_query)
    conn.commit()


def postgres_transactions(times, query_type, x):
    # Create a list of threads

    threads = []

    start = time.time()
    # Create and start a thread for each set of arguments
    for i in range(0, times):
        if query_type == 1:
            thread = threading.Thread(target=postgres_client_join, args=(x))
            threads.append(thread)
        elif query_type == 2:
            thread = threading.Thread(target=postgres_client_integer, args=(x))
            threads.append(thread)
        else:
            thread = threading.Thread(target=postgres_client_string, args=(x))
            threads.append(thread)

    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    return (time.time() - start) * 1000


def main(k):
    with open('output2_'+str(k)+'0000_select_join.csv', mode='w') as file:
        writer = csv.writer(file)
        return_time = duck_transactions(1, 1,k)
        writer.writerow(['duckdb', 1, return_time])
        return_time = duck_transactions(5, 1,k)
        writer.writerow(['duckdb', 5, return_time])
        return_time = duck_transactions(10, 1,k)
        writer.writerow(['duckdb', 10, return_time])
        return_time = duck_transactions(15, 1,k)
        writer.writerow(['duckdb', 15, return_time])
        return_time = duck_transactions(20, 1,k)
        writer.writerow(['duckdb', 20, return_time])
        return_time = postgres_transactions(1, 1,k)
        writer.writerow(['postgres', 1, return_time])
        return_time = postgres_transactions(5, 1,k)
        writer.writerow(['postgres', 5, return_time])
        return_time = postgres_transactions(10, 1,k)
        writer.writerow(['postgres', 10, return_time])
        return_time = postgres_transactions(15, 1,k)
        writer.writerow(['postgres', 15, return_time])
        return_time = postgres_transactions(20, 1,k)
        writer.writerow(['postgres', 20, return_time])



if __name__ == '__main__':
    for z in range(1, 11):
        main(z)
