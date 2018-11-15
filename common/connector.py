import pandas as pd
import yaml
import sqlalchemy

with open('/var/www/bigdata/conf/db.yaml', 'r') as f:
    conf = yaml.load(f)

# sql_name = 'SQL_LEGO'


class db(object):

    def __init__(self, sql_name='SQL_LUBANG'):
        self.sql_name = sql_name
        self.conn = self.make_connection()

    def __getitem__(self, item):
        tb_name = conf[self.sql_name]['table'][item]
        return pd.read_sql_table(tb_name, self.conn)

    def __call__(self, sql):
        return pd.read_sql_query(sql, self.conn)

    def make_connection(self):
        if conf[self.sql_name]['type'] == 'mssql':
            conn = sqlalchemy.create_engine("mssql+pymssql://%s:%s@%s:%s/%s?charset=utf8" % (
                conf[self.sql_name]['user'],
                conf[self.sql_name]['pwd'],
                conf[self.sql_name]['ip'],
                conf[self.sql_name]['port'],
                conf[self.sql_name]['db']))
        else:
            conn = sqlalchemy.create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (
                conf[self.sql_name]['user'],
                conf[self.sql_name]['pwd'],
                conf[self.sql_name]['ip'],
                conf[self.sql_name]['port'],
                conf[self.sql_name]['db']))
        return conn

    def insert_data(self, df, tb_name, if_exists='append'):
        name = conf[self.sql_name]['table'][tb_name] if tb_name in conf[self.sql_name]['table'] else tb_name

        df.to_sql(name=name, con=self.conn, flavor=None, if_exists=if_exists, index=False)
