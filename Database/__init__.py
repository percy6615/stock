from Initial import log_setting
import psycopg2
import pymysql

logger = log_setting.logger

class Mysqls:
    # getInstance
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.conn = None
        self.cur = None
        self._conn()

    def _conn(self):
        logger.info('configure Mysqls database settings')
        self.conn = pymysql.connect(host='localhost', user='percy6615', passwd='l9010220718', db='stock', port=3306)
        self.cur = self.conn.cursor()


    def run(self,sql):
        logger.debug('execute Postgres SQL {}'.format(sql))
        if( self.conn==None):
            self._conn()
        self.cur.execute(sql)
        self.conn.commit()
        self._logout()

    def get(self,sql):
        logger.info('retrieve data from Postgres table')
        if( self.conn==None):
            self._conn()
        self.cur.execute(sql)
        data = self.cur.fetchall()
        self.conn.commit()
        self._logout()
        return data  # return tuple

    def _logout(self):
        try:
            if(self.conn!=None):
                self.conn.close()
            logger.info('log out from Postgres database')
        except None:
            logger.warning('already logged out from Postgres database')


class Postgres:

    @staticmethod
    def _conn():
        logger.info('configure Postgres database settings')
        con = psycopg2.connect(database='stock', user='postgres', password='postgres', host='localhost'
                                    , port='5432')
        cur = con.cursor()
        return con, cur

    @staticmethod
    def run(sql):
        con, cur = Postgres._conn()
        logger.debug('execute Postgres SQL {}'.format(sql))
        cur.execute(sql)
        con.commit()
        Postgres._logout(con)

    @staticmethod
    def get(sql):
        con, cur = Postgres._conn()
        logger.info('retrieve data from Postgres table')
        cur.execute(sql)
        data = cur.fetchall()
        con.commit()
        Postgres._logout(con)
        return data  # return tuple

    @staticmethod
    def _logout(obj):
        try:
            obj.close()
            logger.info('log out from Postgres database')
        except None:
            logger.warning('already logged out from Postgres database')
