from Initial import log_setting
from sqlalchemy import create_engine


logger = log_setting.logger

username = 'percy6615'     # 資料庫sqlalchemy import create_engine帳號
password = 'l9010220718'     # 資料庫密碼
host = 'localhost'    # 資料庫位址
port = '3306'         # 資料庫埠號
database = 'stock'   # 資料庫名稱

def create_ng():
    logger.info('configure database settings for pandas')
    engine = create_engine(r'postgresql://postgres:postgres@localhost:5432/stock')
    return engine

def create_ng_mysql():
    logger.info('configure database settings for pandas')
    engine = create_engine('mysql+pymysql://percy6615:l9010220718@localhost:3306/stock?charset=utf8')
    return engine

def txdate():
    d = '2020-07-20'
    logger.info('txdate is {}'.format(d))
    return d

