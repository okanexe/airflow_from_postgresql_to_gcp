import psycopg2

conn = psycopg2.connect("dbname=postgres user=okans password=")

cur = conn.cursor()

#cur.execute('SELECT version()')

try:
    cur.execute('select * from ordertable')
except :
    conn.rollback()
else:
    conn.commit()

db_version = cur.fetchall()
print(db_version)
