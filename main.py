import duckdb
import threading

def my_function():
    print("Starting my_function...")
    # do some work here

    cursor = duckdb.connect()
    print(cursor.execute('SELECT 42').fetchall())
    print("Finished my_function!")

threads = []
for i in range(5):
    # create a new thread and append it to the list
    thread = threading.Thread(target=my_function)
    threads.append(thread)

# start all threads
for thread in threads:
    thread.start()

# wait for all threads to finish
for thread in threads:
    thread.join()

print("All threads finished!")