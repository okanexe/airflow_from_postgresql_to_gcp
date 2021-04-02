import psycopg2

conn = psycopg2.connect("dbname=postgres user=okans password=")

cur = conn.cursor()
s = "select * from ordertable"
# Use the COPY function on the SQL we created above.
SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)

# Set up a variable to store our file path and name.
t_path_n_file = "/Users/okans/Desktop/first.csv"
with open(t_path_n_file, 'w') as f_output:
    cur.copy_expert(SQL_for_file_output, f_output)

conn.close()
cur.close()