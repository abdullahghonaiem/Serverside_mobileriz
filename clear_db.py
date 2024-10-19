import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('products.db')  # Change to your actual database file
cursor = conn.cursor()

# Drop tables if they exist
cursor.execute("DROP TABLE IF EXISTS products")
cursor.execute("DROP TABLE IF EXISTS vendors")
cursor.execute("DROP TABLE IF EXISTS categories")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Database cleared successfully!")
