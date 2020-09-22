import psycopg2
import pandas as pd
import requests
from sqlalchemy import create_engine


def stock_price():
    con = psycopg2.connect(database='stock', user='postgres', password='postgres', host='localhost', port='5432')
    cur = con.cursor()
    cur.execute("select * from stock_price where stock_id = '2330';")
    data = cur.fetchall()
    con.commit()
    con.close()
    return data


def crawler():
    url = 'http://api.finmindtrade.com/api/v2/data'
    payload = {'dataset': 'TaiwanStockPrice', 'stock_id': '2330', 'date': '2020-07-17'}
    res = requests.get(url, params=payload)
    temp = res.json()
    data = pd.DataFrame(temp['data'])
    print(data)
    # data.to_sql(name='stock_price', con=engine, if_exists='append', index=False)


# engine = create_engine(r'postgresql://postgres:postgres@localhost:5432/stock')
crawler()
