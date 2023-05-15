import psycopg2
import time

conn = psycopg2.connect(
    host="localhost",
    database="bpi",
    user="bpi",
    password="123"
)

num_iterations = 100

cpu_output_file = 'cpu_time.txt'
wall_output_file = 'wall_time.txt'


cursor = conn.cursor()

cpu_times = []
wall_times = []
for i in range(num_iterations):
    start_cpu = time.process_time()
    start_wall = time.time()

    cursor.execute('SELECT * FROM book;')

    end_cpu = time.process_time()
    end_wall = time.time()

    cpu_time = end_cpu - start_cpu
    wall_time = end_wall - start_wall

    cpu_times.append(cpu_time)
    wall_times.append(wall_time)

cpu_mean = sum(cpu_times) / num_iterations

wall_mean = sum(wall_times) / num_iterations

with open(cpu_output_file, 'w') as cpu_file:
    cpu_file.write(f'{cpu_mean}\n')

with open(wall_output_file, 'w') as wall_file:
    wall_file.write(f'{wall_mean}\n')


cursor.close()
conn.close()

print(f'Mean CPU time saved to {cpu_output_file}')
print(f'Mean wall time saved to {wall_output_file}')