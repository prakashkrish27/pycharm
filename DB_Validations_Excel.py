import pandas as pd
import psycopg2 as pg
import mysql.connector as sql
import warnings as wr
import csv
import xlsxwriter
from pandas import DataFrame

src_conn = sql.connect(host="source.c74pc9qqlwja.us-east-1.rds.amazonaws.com", database = 'source',user="admin", passwd="admin2727",use_pure=True)
tgt_conn = pg.connect(host="target.c74pc9qqlwja.us-east-1.rds.amazonaws.com", dbname = 'target',user="postgres", password="admin2727")

wr.filterwarnings("ignore")

src_ip = open("C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/input/Source.txt",'r')
tgt_ip = open("C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/input/target.txt",'r')

src_op = open('C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/output/source_result.csv', 'r+',newline='')
src_op.truncate()

tgt_op = open('C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/output/target_result.csv', 'r+',newline='')
tgt_op.truncate()

for line in src_ip:
    for line1 in tgt_ip:
        df_tgt = pd.read_sql_query(line1, con=tgt_conn)
        df_target = pd.DataFrame(df_tgt)
        print(df_target.to_string(index=False))
        for i,row in df_target.iterrows():
            df_target.to_csv(tgt_op,index=False,header=False,)

    df_src = pd.read_sql_query(line, con=src_conn)
    df_source = pd.DataFrame(df_src)
    print(df_source.to_string(index=False))
    for j,row in df_source.iterrows():
        df_source.to_csv(src_op,index=False,header=False)

src_op.close()
tgt_op.close()










