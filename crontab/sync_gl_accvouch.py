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
    sql = "select i_id,csign,csup_id,isignseq,ino_id,inid,dbill_date,idoc,cbill,ccheck,cbook,ibook,cdigest,ccode,md,mc,md_f,mc_f,dt_date,cdept_id,cperson_id,ccode_equal,daudit_date,iyear,iYPeriod,tvouchtime from GL_accvouch"
    data = pd.read_sql_query(sql, db_u8.conn)
    print(data.head())
    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'fin_gl_accvouch', if_exists='replace')
    print("------ Main End -------")


if __name__ == "__main__":
    main()