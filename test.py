import sys

import calendar
import datetime
import pandas as pd

sys.path.append('/var/www/bigdata/')
print("-----sys-----", sys.path)
# i-synergy 目标数据库（Mysql）
from common import connector

# u8 db
db_u8 = connector.db('SQL_U8')

# i-synergy db
db_ice = connector.db('SQL_LUBANG')

# guo bang big data
db_big_data = connector.db('SQL_TEST')

# 当前时间
_d_now_date = datetime.datetime.now()


def main():
    arr = [0.0, 0.0, 12911.43, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2264.15, 0.0, 4588.45, 100.0, 20773.51, 0.0, 1142.22, 84.0, 0.0, 0.0, 2936.45]

    total = 0
    for v in arr:
        total += v

    print(total)
    #print(calendar.mdays[_d_now_date.month])
    return

    print('------ main start -------')
    sql = "SELECT * FROM ICE_DEVICE_RENT_INFO_HISTORY AS h "
    # 从u8上获取数据
    data = pd.read_sql_query(sql, db_ice.conn)
    print(data.head())

    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'ICE_DEVICE_RENT_INFO_HISTORY', if_exists='replace')
    print("------ Main End -------")

    print('------ main start -------')
    sql = "SELECT * FROM ICE_arap AS i "
    # 从u8上获取数据
    data = pd.read_sql_query(sql, db_ice.conn)
    print(data.head())

    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'ICE_arap', if_exists='replace')
    print("------ Main End -------")


if __name__ == "__main__":
    main()
