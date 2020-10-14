import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="localhost"
    , database="postgres"
    , user="pulsarcon"
    , password="a")  


#read in the data into pandas
#def read_data():
sql = 'select * from accel_data'
pd1 = pd.read_sql_query(sql, conn)

#print a few records
print(pd1)

#order the pd data by device and time
pd1.sort_values(by=['device_id', 'device_t'])

#drop device 15, which has too much noise
#pd2

print(pd1)
    
#conn.commit()
#cur.close()

conn.close()
