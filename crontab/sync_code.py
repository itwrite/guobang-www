import sys
sys.path.append('/var/www/bigdata/')

print("-----sys-----")

import pandas as pd

# i-synergy 目标数据库（Mysql）
from common import connector

# u8 db
db_u8 = connector.db('SQL_U8')

# i-synergy db
db_ice = connector.db('SQL_LUBANG')

# guo bang big data
db_big_data = connector.db('SQL_TEST')


def main():
    print('------ main start -------')
    # 从u8上获取数据
    sql = "select i_id,cclass,cclass_engl,ccode,ccode_name,igrade,bperson,bdept,bend,bcash,iyear from Code"
    data = pd.read_sql_query(sql, db_u8.conn)
    print(data.head())
    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'fin_code', if_exists='replace')
    print("------ Main End -------")


if __name__ == "__main__":
    main()