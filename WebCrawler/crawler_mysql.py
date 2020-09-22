import requests
import Config
import FinMind
from Database import Postgres,Mysqls
from Initial import log_setting
import pandas as pd
import time

logger = log_setting.logger


class Fin:

    def __init__(self):
        self.engine = Config.create_ng_mysql()
        self.url = FinMind.api()
        self.date = Config.txdate()
        mysqls = Mysqls()
        self.pool = mysqls.get("select distinct stock_id from stock_info where length(stock_id) = 4 and industry_category <> 'ETF' order by stock_id asc;")

    def taiwanstockprice(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get TaiwanStockPrice: {}'.format(sid))
            #Mysqls().run("delete from stock_price where stock_id = '{}' and date >= '{}';".format(sid, self.date))
            payload = {'dataset': 'TaiwanStockPrice', 'stock_id': sid, 'date': self.date}
            res = requests.get(self.url, params=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_price', con=self.engine, if_exists='append', index=False)
            time.sleep(10)

    def financialstatements(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get FinancialStatements: {}'.format(sid))
            # Postgres.run("delete from stock_price where stock_id = '{}' and date = '{}';".format(sid, self.date))
            payload = {'dataset': 'FinancialStatements', 'stock_id': sid}
            res = requests.get(self.url, verify=True, data=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_fin_stat', con=self.engine, if_exists='append', index=False)
            time.sleep(10)

    def taiwanstockper(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get {} PER/PBR'.format(sid))
            Mysqls().run("delete from stock_per_pbr where stock_id = '{}' and date > '{}';".format(sid, self.date))
            payload = {'dataset': 'TaiwanStockPER', 'stock_id': sid, 'date': self.date}
            res = requests.get(self.url, params=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_per_pbr', con=self.engine, if_exists='append', index=False)
            time.sleep(10)

    def taiwanstockinfo(self):
        logger.info(__name__)
        logger.info('get stock information')
        payload = {'dataset': 'TaiwanStockInfo'}
        res = requests.get(self.url, params=payload)
        temp = res.json()
        data = pd.DataFrame(temp['data'])
        data.to_sql(name='stock_info', con=self.engine, if_exists='append', index=False)

    def stockdividend(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get {} stock dividend'.format(sid))
            # Postgres.run("delete from stock_dividend where stock_id = '{}' and date = '{}';".format(sid, self.date))
            payload = {'dataset': 'StockDividend', 'stock_id': sid}
            res = requests.get(self.url, params=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_dividend', con=self.engine, if_exists='append', index=False)
            time.sleep(10)

    def stocksecurities(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get {} stock dividend'.format(sid))
            Mysqls().run("delete from stock_securities where stock_id = '{}' and date >= '{}';".format(sid, self.date))
            payload = {'dataset': 'TaiwanStockMarginPurchaseShortSale', 'stock_id': sid, 'date': self.date}
            res = requests.get(self.url, params=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_securities', con=self.engine, if_exists='append', index=False)
            time.sleep(10)

    def stockmonthrevenue(self):
        logger.info(__name__)
        for i in self.pool:
            sid = i[0]
            logger.info('get {} stock month revenue'.format(sid))
            Mysqls().run("delete from stock_mom_revenue where stock_id = '{}' and date >= '{}';".format(sid, self.date))
            payload = {'dataset': 'TaiwanStockMonthRevenue', 'stock_id': sid, 'date': self.date}
            res = requests.get(self.url, params=payload)
            temp = res.json()
            data = pd.DataFrame(temp['data'])
            data.to_sql(name='stock_mom_revenue', con=self.engine, if_exists='append', index=False)
            time.sleep(10)
