import duckdb
import time

con = duckdb.connect(database="bookstore.db", read_only=True)

# Number of iterations
num_iterations = 100

# Output file paths
cpu_output_file = 'cpu_time_duck.txt'
wall_output_file = 'wall_time_duck.txt'

# Connect to the DuckDB database
conn = duckdb.connect(database)

# Create a cursor
cursor = conn.cursor()

# Perform the query iterations and measure time
cpu_times = []
wall_times = []
for i in range(num_iterations):
    # Execute the query and measure the time
    start_cpu = time.process_time()
    start_wall = time.time()

    cursor.execute('SELECT * FROM book')

    end_cpu = time.process_time()
    end_wall = time.time()

    # Calculate the time differences
    cpu_time = end_cpu - start_cpu
    wall_time = end_wall - start_wall

    cpu_times.append(cpu_time)
    wall_times.append(wall_time)

# Calculate the mean of CPU times
cpu_mean = sum(cpu_times) / num_iterations

# Calculate the mean of wall times
wall_mean = sum(wall_times) / num_iterations

# Save the mean CPU time to the output file
with open(cpu_output_file, 'w') as cpu_file:
    cpu_file.write(f'{cpu_mean}\n')

# Save the mean wall time to the output file
with open(wall_output_file, 'w') as wall_file:
    wall_file.write(f'{wall_mean}\n')

# Close the cursor and connection
cursor.close()
conn.close()

print(f'Mean CPU time saved to {cpu_output_file}')
print(f'Mean wall time saved to {wall_output_file}')