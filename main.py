import duckdb
import threading

def my_function(x):
    cursor = duckdb.connect(database="bookstore10000.db")
    query = "INSERT INTO publisher VALUES ('blas', {} );"
    query = query.format(x)
    cursor.execute(query)

    cursor.commit()


threads = []
for i in range(5):
    # create a new thread and append it to the list
    thread = threading.Thread(target=my_function, args=(i+10000+1,))
    threads.append(thread)

# start all threads
for thread in threads:
    thread.start()

# wait for all threads to finish
for thread in threads:
    thread.join()

print("All threads finished!")