import psycopg2

# Connect to the database
conn = psycopg2.connect(
        host="localhost",
        database="bookstore",
        user="postgres",
        password="root"
    )

# Create a cursor
cur = conn.cursor()

# Execute a query to get all book titles
cur.execute("SELECT title FROM book")

# Fetch all book titles
titles = cur.fetchall()

# Close the cursor and database connection
cur.close()
conn.close()

# Create a dictionary to count the occurrences of each word
word_count = {}

# Loop through each title
for title in titles:
    # Split the title into individual words
    words = title[0].lower().split()
    # Loop through each word
    for word in words:
        # If the word is not already in the dictionary, add it with a count of 1
        if word not in word_count:
            word_count[word] = 1
        # If the word is already in the dictionary, increment its count
        else:
            word_count[word] += 1

# Sort the words by their count in descending order
sorted_words = sorted(word_count.items(), key=lambda x: x[1], reverse=True)

# Get the 100 most common words
most_common_words = [word[0] for word in sorted_words[:100]]

# Print the 100 most common words
print(most_common_words)
