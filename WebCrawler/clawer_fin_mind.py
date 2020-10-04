

import requests
import pandas as pd
import Config
engine = Config.create_ng_mysql()
from datetime import date
import datetime
class FinMindDataSetEnum():
        TaiwanStockInfo = {'type':'Tech','eng':'TaiwanStockInfo','cht':'台股總覽',}
        TaiwanStockPrice = {'type':'Tech','eng':'TaiwanStockPrice','cht':'股價日成交資訊'}
        TaiwanStockPriceMinute =  {'type':'Tech','eng':'TaiwanStockPriceMinute','cht':'即時股價'}
        TaiwanStockPriceMinuteBidAsk = {'type':'Tech','eng':'TaiwanStockPriceMinuteBidAsk','cht':'即時最佳五檔','complex':True}
        TaiwanStockPER= {'type':'Tech','eng':'TaiwanStockPER','cht':'個股PER、PBR'}
        StatisticsOfOrderBookAndTrade = {'type':'Tech','eng':'StatisticsOfOrderBookAndTrade','cht':'每5秒委託成交統計','complex':True}
        TaiwanVariousIndicators5Seconds =  {'type':'Tech','eng':'TaiwanVariousIndicators5Seconds','cht':'加權指數'}
        TaiwanStockMarginPurchaseShortSale  =  {'type':'Bargain','eng':'TaiwanStockMarginPurchaseShortSale','cht':'台灣市場整體融資融劵表'}
        InstitutionalInvestorsBuySell = {'type':'Bargain','eng': 'InstitutionalInvestorsBuySell', 'cht': '法人買賣表'}
        InstitutionalInvestors = {'type':'Bargain','eng': 'InstitutionalInvestors', 'cht': '台灣市場整體法人買賣表'}
        Shareholding = {'type':'Bargain','eng': 'Shareholding', 'cht': '股東結構表'}
        TaiwanStockHoldingSharesPer = {'type':'Bargain','eng': 'TaiwanStockHoldingSharesPer', 'cht': '股東持股分級表'}
        SecuritiesLending = {'type':'Bargain','eng': 'SecuritiesLending', 'cht': '借券成交明細'}
        TotalMarginPurchaseShortSale = {'type':'Bargain','eng': 'TotalMarginPurchaseShortSale', 'cht': '台灣市場整體融資融劵表'}
        FinancialStatements = {'type':'Fundamentals','eng': 'FinancialStatements', 'cht': '綜合損益表'}
        BalanceSheet = {'type':'Fundamentals','eng': 'BalanceSheet', 'cht': '資產負債表'}
        TaiwanCashFlowsStatement = {'type':'Fundamentals','eng': 'TaiwanCashFlowsStatement', 'cht': '現金流量表'}
        StockDividend = {'type':'Fundamentals','eng': 'StockDividend', 'cht': '股利政策表'}
        StockDividendResult = {'type':'Fundamentals','eng': 'StockDividendResult', 'cht': '除權除息結果表'}
        TaiwanStockMonthRevenue = {'type':'Fundamentals','eng': 'TaiwanStockMonthRevenue', 'cht': '月營收表'}
        TaiwanFutOptTickInfo  =  {'type':'Derivative','eng':'TaiwanFutOptTickInfo','cht':'期貨、選擇權即時報價總覽','complex':True}
        TaiwanFutOptTick = {'type':'Derivative','eng': 'TaiwanFutOptTick', 'cht': '期貨、選擇權即時報價'}
        TaiwanOptionFutureInfo = {'type':'Derivative','eng': 'TaiwanOptionFutureInfo', 'cht': '期貨、選擇權日成交資訊總覽'}
        TaiwanFuturesDaily = {'type':'Derivative','eng': 'TaiwanFuturesDaily', 'cht': '期貨日成交資訊'}
        TaiwanOptionDaily = {'type':'Derivative','eng': 'TaiwanOptionDaily', 'cht': '選擇權日成交資訊'}
        TaiwanFuturesTick = {'type':'Derivative','eng': 'TaiwanFuturesTick', 'cht': '期貨交易明細表','complex':True}
        TaiwanOptionTick = {'type':'Derivative','eng': 'TaiwanOptionTick', 'cht': '選擇權交易明細表','complex':True}
        TaiwanStockNews = {'type':'Others','eng': 'TaiwanStockNews', 'cht': '相關新聞表'}

class FinCrawler:
    def __init__(self):
        self.dataurl ='http://api.finmindtrade.com/api/v3/data'
        self.datalisturl = 'https://api.finmindtrade.com/api/v3/datalist'
        self.translationurl = 'https://api.finmindtrade.com/api/v3/translation'
        self.pool = self.initTaiwanStockInfo()

    def datalist(self,dataset,user_id,password):
        payload = {'dataset': dataset, 'user_id': user_id, 'password': password}
        data = self.getPandasData(self.datalisturl, payload)
        return data

    def data(self, dataset, stock_id, startdate, enddate, user_id, password):
        payload = {'dataset': dataset, 'stock_id': stock_id, 'date':startdate,
                   'end_date':enddate,'user_id':user_id,'password':password }
        data = self.getPandasData(self.dataurl, payload)
        return data

    def translation(self,dataset,user_id,password):
        payload = {'dataset': dataset, 'user_id': user_id, 'password': password}
        data = self.getPandasData(self.translationurl,payload)
        return data

    def getPandasData(self,url, payload):
        res = requests.get(url, params=payload)
        temp = res.json()
        data = pd.DataFrame(temp['data'])
        if 'date' not in data:
            today = date.today()
            d1 = today.strftime("%Y-%m-%d")
            data['date'] = d1
        return data

    def initTaiwanStockInfo(self):
        dataset = FinMindDatasetEnum.TaiwanStockInfo['eng']
        payload = {'dataset': dataset, 'stock_id': None, 'date': None,
                   'end_date': None, }
        if not engine.dialect.has_table(engine, table_name=dataset):
            data = self.getPandasData(self.dataurl,payload)
            data.to_sql(name=dataset.lower(), con=engine, if_exists='append', index=False)
        else:
            querysql = "SELECT count(*) as countdate FROM stock.taiwanstockinfo a " \
                       "where date_format(a.date,'%%Y-%%m') = date_format(CURDATE(),'%%Y-%%m') "
            querycount = pd.read_sql_query(sql=querysql,
                           con=engine.connect(),
                           )
            if (len(querycount) == 1 and querycount['countdate'][0] > 0) :
                pass
            else:
                data = self.getPandasData(self.dataurl, payload)
                data.to_sql(name=dataset.lower(), con=engine, if_exists='replace', index=False)
        querysql = "SELECT distinct stock_id FROM stock.taiwanstockinfo a " \
                          "where length(stock_id) = 4 and industry_category not in  ('ETF','封閉式基金') order by stock_id asc; "
        querydata = pd.read_sql(sql=querysql, con=engine.connect(),)
        returnarry = []
        for i in querydata['stock_id']:
            returnarry.append(i)
        return returnarry

    def createDataPayload(self, dataset, stock_id):
    #     if not engine.dialect.has_table(engine, table_name=dataset):
            initpayload =  {'dataset': dataset, 'stock_id': stock_id, 'date':startdate,
                   'end_date':enddate,'user_id':user_id,'password':password }



# t1 = pd.DataFrame({'year': [1, 2, 3], 'month': [4, 5, 6], 'value': [2, 2, 2]})
# print(t1.to_dict())
# querysql = "SELECT distinct stock_id FROM stock.taiwanstockinfo a " \
#                            "where length(stock_id) = 4 and industry_category not in  ('ETF','封閉式基金')" \
#                             "and type = 'twse' order by stock_id asc; "
# data = pd.read_sql_query(sql=querysql,
#                            con=engine.connect(),
#                            )
# print(data)

# jsondata ={"msg":"success","status":200,"data":[{"industry_category":"ETF","stock_id":"0001","stock_name":"鴻運","type":"twse"},{"industry_category":"封閉式基金","stock_id":"0002","stock_name":"福元","type":"twse"},{"industry_category":"封閉式基金","stock_id":"0003","stock_name":"成長","type":"twse"},{"industry_category":"封閉式基金","stock_id":"0004","stock_name":"國民","type":"twse"}]}
#
# ppp = pd.DataFrame(jsondata['data']);
# print(ppp)

# import threading
# import time
# def job(num):
#     h = 21
#     m = 00
#     while True:
#         now =  datetime.datetime.now()
#         # print(now.hour, now.minute)
#         if now.hour == h and now.minute == m:
#             print("oj")
#
#         # 每隔60秒檢測一次
#         time.sleep(60)
# x=0
# threads=threading.Thread(target = job, args =(x,))
# threads.start()
# print(threading.active_count())
# threads.join()
# print("Done.")
