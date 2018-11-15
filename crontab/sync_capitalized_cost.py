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


def get_capitalized_cost_sql():
    sql = "  SELECT distinct dls.AutoID"
    sql += "  ,ISNULL(iass.iProcessFee, 0) as 加工费"
    sql += "  ,ISNULL(iass.imaterialfee, 0) as 材料费"

    sql += "  ,iass.fLaborStdCostE as 标准成本_标准人工费"
    sql += "  ,iass.fManuFixStdCostE as 标准成本_固定加工费"
    sql += "  ,inv.cInvCCode as 存货分类编码"

    sql += "  ,dl.cDLCode AS 发退货单号  "
    sql += "  ,dl.cCusCode AS 客户编码  "
    sql += "  ,cus.cCusAbbName AS 客户简称  "
    sql += "  ,dl.cexch_name AS 币种  "
    sql += "  ,dls.cInvCode AS 存货编码  "
    sql += "  ,dls.cInvName AS 存货名称  "
    sql += "  ,dls.iQuantity AS 发货数量  "
    sql += "  ,dls.iMoney AS 发货无税金额  "
    sql += "  ,dls.iSettleNum AS 发货含税金额  "
    sql += "  ,dls.iNatDisCount AS 发货折扣  "
    sql += "  ,dls.fVeriBillQty AS 开票数量  "
    sql += "  ,dls.fVeriBillSum AS 开票金额  "

    # 未开票数量 = 数量 - 开票数量
    sql += "  ,(dls.iQuantity - dls.fVeriBillQty) AS 未开票数量  "

    # 未开票金额 = 未开票数量 x 原币含税单价
    sql += "  ,(dls.iQuantity - dls.fVeriBillQty)*dls.iTaxUnitPrice AS 未开票金额  "

    # 已审核开票原币金额
    sql += "  ,(dls.fVeriBillSum) AS 回款金额  "

    # 未回款金额 = 原币含税单价 x 数量 - 回款金额
    sql += "  ,(dls.iQuantity * dls.iTaxUnitPrice - dls.fVeriBillSum) AS 未回款金额  "

    # 数量
    sql += "  ,dls.iQuantity AS 成本单价数量  "

    #
    sql += "  ,iass.iOutCost AS 成本单价  "
    # 成本金额 = 成本单价 x 数量
    sql += "  ,(iass.iOutCost*dls.iQuantity) AS 成本金额  "
    sql += "  ,(dls.iNatMoney-iass.iOutCost*dls.iQuantity) AS 发货毛利  "
    sql += "  ,(dls.fVeriBillSum-iass.iOutCost*dls.fVeriBillQty) AS 发货回款毛利  "
    sql += "  ,dl.dDate AS 日期  "
    sql += "  ,LEFT(CONVERT(varchar(100),dl.dDate, 23),7) AS 月份  "
    sql += "  ,iass.iOutCost AS 材料成本单价  "
    sql += "  ,(iass.iOutCost * dls.iQuantity) AS 销售材料成本  "
    sql += "  ,dls.cInvName AS 产品分类  "
    sql += "  ,(case when cus.bCusOverseas = 1 or cus.cCCCode = '02001' then '国外' else '国内' end) AS 销售区域  "
    sql += "  ,(case when cus.bCusOverseas = 1 or cus.cCCCode = '02001' then '国外' else '国内' end) AS 国外国内  "

    sql += "  FROM DispatchLists dls   "
    sql += "  JOIN DispatchList dl ON(dl.DLID = dls.DLID)   "
    sql += "  JOIN Customer cus ON(cus.cCusCode = dl.cCusCode)   "
    sql += "  JOIN SO_SODetails sod ON(sod.iSOsID = dls.iSOsID)   "
    sql += "  JOIN IA_Subsidiary iass ON(iass.idlsid = dls.iDLsID)   "
    sql += "  JOIN Inventory inv  ON(inv.cInvCode = dls.cInvCode)  "
    sql += "  WHERE   "
    sql += "  dl.cCusCode !='' "

    return sql


def main():
    print('------ main start -------')
    # 从u8上获取数据
    sql = get_capitalized_cost_sql()
    print(sql)
    data = pd.read_sql_query(sql, db_u8.conn)

    # print(data.head())
    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'fin_capitalized_cost', if_exists='replace')
    print("------ Main End -------")


if __name__ == "__main__":
    main()