import psycopg2
from google.cloud import storage
import pandas as pd
from datetime import timedelta, datetime


def date_range(start_date, end_date):
  rang = ( datetime.strptime(end_date, '%Y-%m-%d').date() - datetime.strptime(start_date, '%Y-%m-%d').date() ).days
  return rang

                                              

conn = psycopg2.connect("dbname=postgres user=okans password=")

cur = conn.cursor()

# table name must be the same in DB and in dates.csv table
tablename='ordertable'
TABLES = []
#df = pd.read_csv('dates.csv')
#from_csv_date = df[tablename]
#current_date = str(datetime.today().date())

start_date='2020-01-01'
end_date = '2020-01-03'

try:
    # between'de altsınırı alır üst sınırı almaz
    if (date_range(start_date, end_date) >= 1):
        date = datetime.strptime(start_date, '%Y-%m-%d').date()
        rang = date_range(start_date, end_date)
        #print(rang)
        for i in range(1, rang+1):
            current_date = str(date)
            query = " select * from {} where created_at between \'{}\' and \'{}\' ".format(tablename, str(date), str(date + timedelta(1)))
            date = date + timedelta(1)
            TABLES.append(str(current_date))
            print(date)
            #query = "select * from ordertable where created_at < \'{}\' ".format("2020-01-05")
            SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
            print(current_date)
            print(i)
            # Set up a variable to store our file path and name.
            t_path_n_file = "/Users/okans/Desktop/{}.csv".format(current_date)
            with open(t_path_n_file, 'w') as f_output:
                cur.copy_expert(SQL_for_file_output, f_output)
    else:
        print("...enter correct parameter...")

except :
    print("FAILED !!!!!")
    conn.rollback()
else:
    # en son atılan datayı tutmak için current_date csv'ye atılacak.
    #df[tablename] = current_date
    conn.commit()
print(TABLES)
conn.close()
cur.close()


