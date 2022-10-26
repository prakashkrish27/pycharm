import pandas as pd
import psycopg2 as pg
import mysql.connector as sql
import warnings as wr

src_conn = sql.connect(host="source.c74pc9qqlwja.us-east-1.rds.amazonaws.com", database = 'source',user="admin", passwd="admin2727",use_pure=True)
tgt_conn = pg.connect(host="target.c74pc9qqlwja.us-east-1.rds.amazonaws.com", dbname = 'target',user="postgres", password="admin2727")

src_ip = open("C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/input/Source.txt",'r')
tgt_ip = open("C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/input/target.txt",'r')

src_output = "C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/output/main_source.csv"
tgt_output = "C:/Users/Prakash.Krishnan/OneDrive - ACS Solutions/Desktop/PyCharm/output/main_target.csv"

wr.filterwarnings("ignore")

main_src = open(src_output, 'r+',newline='')
main_src.truncate()

main_tgt = open(tgt_output, 'r+',newline='')
main_tgt.truncate()

headerlist = ['Table_Name','Count']

for line in src_ip:
    for line1 in tgt_ip:
        df_tgt = pd.read_sql_query(line1, con=tgt_conn)
        df_target = pd.DataFrame(df_tgt)
        print(df_target.to_string(index=False))
        for i,row in df_target.iterrows():
            df_target.to_csv(main_tgt,index=False,header=False)

    df_src = pd.read_sql_query(line, con=src_conn)
    df_source = pd.DataFrame(df_src)
    print(df_source.to_string(index=False))
    for j,row in df_source.iterrows():
        df_source.to_csv(main_src,index=False,header=False)

main_src.close()
main_tgt.close()

sdf = pd.read_csv(src_output,header=None)
sdf.to_csv(src_output,header=headerlist,index=False)
tdf = pd.read_csv(tgt_output,header=None)
tdf.to_csv(tgt_output,header=headerlist,index=False)

src_cmp = sdf.iloc[:, 1].tolist()
tgt_cmp = tdf.iloc[:, 1].tolist()

print(src_cmp)
print(tgt_cmp)

count_validation = list(set(src_cmp) - set(tgt_cmp))

if len(count_validation) > 0:
    print("Count Validation Failed")
    print(count_validation)
else:
    print("Count Validation Passed")
