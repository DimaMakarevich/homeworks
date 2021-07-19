from pyhive import hive
import os
import sys

host_name = "localhost"
port = 10000
user = "admin"
password = "password"
database = "forex_db"
authMechanism = "PLAIN"

#data_path = "/homeworks/4_Hive/fund/"


def hiveconnection(host_name, port, user, password, database):
    with hive.Connection(host=host_name, port=port, username=user, database=database) as conn:
        with conn.cursor() as cur:
            #data_files = os.listdir(data_path)
            #print(data_files)
            # for file_name in data_files:
            query = "LOAD DATA LOCAL INPATH '{}' INTO TABLE forex_db.forex_stg".format(sys.argv[1])
            cur.execute(query)


# cur = conn.cursor()
# query = "LOAD DATA INPATH '/user/hive/warehouse/fund/XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv' INTO TABLE forex_db.forex_stg"
# print(query)
# cur.execute(query)


# print(cur.fetchone())
# print(cur.fetchall())
# cur.fetchall()
# result = cur.fetchall()
# return result


# Call above function
hiveconnection(host_name, port, user, password, database)

# output = hiveconnection(host_name, port, user,password, database)
# print(output)
