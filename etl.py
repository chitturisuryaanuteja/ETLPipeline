import requests
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np 
from datetime import datetime
import sqlite3

log_file="code_log.txt"


def log_progress(message):
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(f"{timestamp} : {message}\n ")


def extract(url,table_attribs):
    page=requests.get(url).text
    data=BeautifulSoup(page,'html.parser')
    df=pd.DataFrame(columns=table_attribs)
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)>=3:
            name = col[1].get_text(strip=True)
            mc_usd = col[2].get_text(strip=True)
            if name and mc_usd:
                data_dict = {
                    "Name": name,
                    "MC_USD_Billion": mc_usd
                }
                df=pd.concat([df,pd.DataFrame(data_dict, index=[0])],ignore_index=True)
    return df

def transform(df,csv_path):
    exchange_rates=pd.read_csv(csv_path)
    exchange_rate_dict=exchange_rates.set_index('Currency').to_dict()['Rate']
    df["MC_USD_Billion"]=pd.to_numeric(df["MC_USD_Billion"],errors='coerce')
    df['MC_GBP_Billion']=[np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion']=[np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion']=[np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]


    return df
    
def load_to_csv(df,output_path='./Largest_banks_data.csv'):
    df.to_csv(output_path,index=False)

def load_to_db(df,sql_connection,table_name):
    with sqlite3.connect(sql_connection) as conn:
        df.to_sql(table_name,conn,if_exists='replace',index=False)
    log_progress("Data loaded into the database successfully.")




def run_query(query_statement,sql_connection):
    with sqlite3.connect(sql_connection) as conn:
        query_output = pd.read_sql(query_statement, conn)
    print(query_output)
    log_progress("Query executed successfully.")



log_progress("Intializing the ETL")
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_csv_path = './Largest_banks_data.csv'

log_progress("Extraction Process started")

df = extract(url, table_attribs)

log_progress("Extract Process ended")

log_progress("Transform Phase Started")

df =transform(df,csv_path)

log_progress("Transform phases ended")
load_to_csv(df,output_csv_path)

log_progress('data saved to csv file')

load_to_db(df,db_name,table_name)

log_progress('Data loaded to Database as a table')


query_statement = f"SELECT * FROM {table_name} ORDER BY MC_USD_Billion DESC LIMIT 10;"
run_query(query_statement,db_name)

log_progress('process Complete')

