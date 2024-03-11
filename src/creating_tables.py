import mysql.connector

# setting first parameters
user = 'root'
password = '123'
host = 'localhost'
database = 'sp_500'

# connection
mysql_connection = mysql.connector.connect(
  host = host,
  user = user,
  password = password,
  database = database
)

cursor = mysql_connection.cursor()

# making the query to create tables
sql_file = open('src/sql/creating_tables.sql', 'r')
query = sql_file.read().replace('\n', '')

# executing all
cursor.execute(query)
cursor.close()
mysql_connection.close()