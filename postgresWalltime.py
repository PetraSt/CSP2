import csv

import psycopg2
import time

queries = ['''SELECT COUNT(book_id)
     FROM book
     WHERE title = 'After the Funeral';''',
           '''SELECT book.title, publisher.publisher_name, book_language.language_name
    FROM book
    JOIN publisher ON book.publisher_id = publisher.publisher_id
    JOIN book_language ON book.language_id = book_language.language_id
    GROUP BY 1,2,3  
    ORDER BY 3;''',
           '''INSERT INTO book (title, book_id, isbn13, language_id, num_pages, publication_date, publisher_id) 
    VALUES ('The Metamorphosis', {}, '8987059752', 2, 276, '1996-09-01', 1010);''',
           ''' UPDATE book
    SET title = 'The Idiot'
    WHERE language_id = 19 AND publisher_id = (SELECT publisher_id FROM publisher WHERE publisher_name = 'Russian literature');''',
           '''DELETE FROM book
    WHERE book_id = {};''']


def run_query(k):
    query_type = 0
    with open('postgres_times_' + str(k) + '0000.csv', mode='w') as file:
        writer = csv.writer(file)
        delete_id = 1234
        insert_id = k * 10000
        for query in queries:
            conn = psycopg2.connect(
                host="localhost",
                database="bookstore" + str(k * 10000),
                user="postgres",
                password="root"
            )

            num_iterations = 100

            cursor = conn.cursor()

            cpu_times = []
            wall_times = []
            for i in range(num_iterations):
                start_cpu = time.process_time()
                start_wall = time.time()
                query2 = query
                if query_type == 2:
                    insert_id += 1
                    query2 = query.format(insert_id)

                if query_type == 4:
                    delete_id -= 1
                    query2 = query.format(delete_id)
                cursor.execute(query2)

                end_cpu = time.process_time()
                end_wall = time.time()

                cpu_time = end_cpu - start_cpu
                wall_time = end_wall - start_wall

                cpu_times.append(cpu_time)
                wall_times.append(wall_time)

            cpu_mean = sum(cpu_times) / num_iterations

            wall_mean = sum(wall_times) / num_iterations
            query_type += 1
            writer.writerow(['query' + str(query_type), cpu_mean, wall_mean])
            cursor.close()
            conn.close()


for x in range(1, 11):
    run_query(x)
