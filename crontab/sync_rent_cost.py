import sys
sys.path.append('/var/www/bigdata/')
print("-----sys-----")

import time
import datetime
import pandas as pd

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


# 获取租金成本数据
# 参数 startTime启租开始时间（毫秒时间戳）启租结束时间（毫秒时间戳） endTime company公司 deviceModel设备型号 keyword合同编号或机身号
# 返回数据 rentCostData租金成本数据表数据(DataFrame) companyData公司数据（） deviceModelData设备型号数据（）


def get_rent_cost(startTime="", endTime="", company="", deviceModel="", keyword=""):
    # print("获取租金成本数据", "startTime:", startTime, "endTime:", endTime, "company:", company, "deviceModel:", deviceModel,
    #       "keyword:", keyword, "filename:", filename)

    # 获取公司数据
    # companyDataSql = "select distinct h.`AGENT_CO` as agentCo from ice_device_rent_info_history h"
    # companyData = pd.read_sql_query(companyDataSql, ice_conn)
    # 获取备型号数据
    # deviceModelDataSql = "select distinct h.`DEVICE_MODEL` as deviceModel from ice_device_rent_info_history h"
    # deviceModelData = pd.read_sql_query(deviceModelDataSql, ice_conn)

    # get_json["company"]=""
    # get_json["deviceModel"]=""
    # get_json["keyword"]=""
    # get_json["startTime"]=int((nowtime-7*24*60*60))*1000
    # get_json["endTime"]=int(nowtime)*1000
    # startTimeD="2017-04-01"
    # endTimeD="2018-06-14"
    # 获取开始和结束时间结束时间减1秒
    # startTime=int(time.mktime(time.strptime(startTimeD,'%Y-%m-%d')))*1000
    # print(startTime)
    # endTime=int((time.mktime(time.strptime(endTimeD,'%Y-%m-%d'))-1))*1000
    sql = "SELECT "
    sql += "  h.`SIGNING_DATE` as signingDate,h.`AGENT_CO` as agentCo,h.`CONTRACT_NO` as "
    sql += "  contractNo,h.contract_id as contractId, "
    sql += "  h.`CUSTOM_CONTRACT_NO` as customContractNo,h.`DEVICE_MODEL` as deviceModel,h.`BODY_NUM` AS "
    sql += "  bodyNum, "
    sql += "  h.`RENT_TIME` as rentTime,h.`RETU_TIME` as retuTime,h.LEASE_TERM as "
    sql += "  leaseTerm,h.PERIOD_NUM AS periodNum, "
    sql += "  h.DEVICE_LEASE_MONEY as deviceLeaseMoney, "
    sql += "  count(distinct (case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then month(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) else null end)) as  yearTicketMonth, "
    sql += "  sum(case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then 1 else 0 end) as  YearTicketNumber, "

    sql += "  MAX(CASE WHEN i.ISSUE='1' and i.ARAP_STATE ='1' THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime1, "
    sql += "  MAX(CASE WHEN i.ISSUE='1' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime1, "
    sql += "  MAX(CASE WHEN i.ISSUE='1' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney1, "
    sql += " MAX(CASE WHEN i.ISSUE='1' and i.ARAP_STATE ='1'  and i.AMOUNT_MONEY is not null  THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney1, "
    sql += "   "
    sql += "  MAX(CASE WHEN i.ISSUE='2' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime2, "
    sql += "  MAX(CASE WHEN i.ISSUE='2' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime2, "
    sql += "  MAX(CASE WHEN i.ISSUE='2' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney2, "
    sql += " MAX(CASE WHEN i.ISSUE='2'and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null  THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney2, "
    sql += "   "
    sql += "  MAX(CASE WHEN i.ISSUE='3' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime3, "
    sql += "  MAX(CASE WHEN i.ISSUE='3' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime3, "
    sql += "  MAX(CASE WHEN i.ISSUE='3' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney3, "
    sql += " MAX(CASE WHEN i.ISSUE='3' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney3, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='4' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime4, "
    sql += "  MAX(CASE WHEN i.ISSUE='4' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime4, "
    sql += "  MAX(CASE WHEN i.ISSUE='4' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney4, "
    sql += " MAX(CASE WHEN i.ISSUE='4' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney4, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='5' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime5, "
    sql += "  MAX(CASE WHEN i.ISSUE='5' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime5, "
    sql += "  MAX(CASE WHEN i.ISSUE='5' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney5, "
    sql += " MAX(CASE WHEN i.ISSUE='5' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney5, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='6' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime6, "
    sql += "  MAX(CASE WHEN i.ISSUE='6' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime6, "
    sql += "  MAX(CASE WHEN i.ISSUE='6' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney6, "
    sql += " MAX(CASE WHEN i.ISSUE='6' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney6, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='7' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime7, "
    sql += "  MAX(CASE WHEN i.ISSUE='7' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime7, "
    sql += "  MAX(CASE WHEN i.ISSUE='7' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney7, "
    sql += " MAX(CASE WHEN i.ISSUE='7' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney7, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='8' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime8, "
    sql += "  MAX(CASE WHEN i.ISSUE='8' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime8, "
    sql += "  MAX(CASE WHEN i.ISSUE='8' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney8, "
    sql += " MAX(CASE WHEN i.ISSUE='8' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney8, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='9' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime9, "
    sql += "  MAX(CASE WHEN i.ISSUE='9' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime9, "
    sql += "  MAX(CASE WHEN i.ISSUE='9' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney9, "
    sql += " MAX(CASE WHEN i.ISSUE='9' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney9, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='10' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime10, "
    sql += "  MAX(CASE WHEN i.ISSUE='10' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime10, "
    sql += "  MAX(CASE WHEN i.ISSUE='10' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney10, "
    sql += " MAX(CASE WHEN i.ISSUE='10' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney10, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='11' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime11, "
    sql += "  MAX(CASE WHEN i.ISSUE='11' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime11, "
    sql += "  MAX(CASE WHEN i.ISSUE='11' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney11, "
    sql += " MAX(CASE WHEN i.ISSUE='11' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney11, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='12' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime12, "
    sql += "  MAX(CASE WHEN i.ISSUE='12' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime12, "
    sql += "  MAX(CASE WHEN i.ISSUE='12' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney12, "
    sql += " MAX(CASE WHEN i.ISSUE='12' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney12, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='13' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime13, "
    sql += "  MAX(CASE WHEN i.ISSUE='13' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime13, "
    sql += "  MAX(CASE WHEN i.ISSUE='13' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney13, "
    sql += " MAX(CASE WHEN i.ISSUE='13' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney13, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='14' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime14, "
    sql += "  MAX(CASE WHEN i.ISSUE='14' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime14, "
    sql += "  MAX(CASE WHEN i.ISSUE='14' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney14, "
    sql += " MAX(CASE WHEN i.ISSUE='14' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney14, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='15' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime15, "
    sql += "  MAX(CASE WHEN i.ISSUE='15' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime15, "
    sql += "  MAX(CASE WHEN i.ISSUE='15' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney15, "
    sql += " MAX(CASE WHEN i.ISSUE='15' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney15, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='16' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime16, "
    sql += "  MAX(CASE WHEN i.ISSUE='16' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime16, "
    sql += "  MAX(CASE WHEN i.ISSUE='16' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney16, "
    sql += " MAX(CASE WHEN i.ISSUE='16' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney16, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='17' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime17, "
    sql += "  MAX(CASE WHEN i.ISSUE='17' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime17, "
    sql += "  MAX(CASE WHEN i.ISSUE='17' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney17, "
    sql += " MAX(CASE WHEN i.ISSUE='17' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney17, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='18' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime18, "
    sql += "  MAX(CASE WHEN i.ISSUE='18' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime18, "
    sql += "  MAX(CASE WHEN i.ISSUE='18' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney18, "
    sql += " MAX(CASE WHEN i.ISSUE='18' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney18, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='19' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime19, "
    sql += "  MAX(CASE WHEN i.ISSUE='19' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime19, "
    sql += "  MAX(CASE WHEN i.ISSUE='19' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney19, "
    sql += " MAX(CASE WHEN i.ISSUE='19' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney19, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='20' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime20, "
    sql += "  MAX(CASE WHEN i.ISSUE='20' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime20, "
    sql += "  MAX(CASE WHEN i.ISSUE='20' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney20, "
    sql += " MAX(CASE WHEN i.ISSUE='20' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney20, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='21' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime21, "
    sql += "  MAX(CASE WHEN i.ISSUE='21' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime21, "
    sql += "  MAX(CASE WHEN i.ISSUE='21' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney21, "
    sql += " MAX(CASE WHEN i.ISSUE='21' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney21, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='22' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime22, "
    sql += "  MAX(CASE WHEN i.ISSUE='22' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime22, "
    sql += "  MAX(CASE WHEN i.ISSUE='22' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney22, "
    sql += " MAX(CASE WHEN i.ISSUE='22' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney22, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='23' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime23, "
    sql += "  MAX(CASE WHEN i.ISSUE='23' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime23, "
    sql += "  MAX(CASE WHEN i.ISSUE='23' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney23, "
    sql += " MAX(CASE WHEN i.ISSUE='23' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney23, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='24' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime24, "
    sql += "  MAX(CASE WHEN i.ISSUE='24' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime24, "
    sql += "  MAX(CASE WHEN i.ISSUE='24' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney24, "
    sql += " MAX(CASE WHEN i.ISSUE='24' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney24, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='25' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime25, "
    sql += "  MAX(CASE WHEN i.ISSUE='25' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime25, "
    sql += "  MAX(CASE WHEN i.ISSUE='25' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney25, "
    sql += " MAX(CASE WHEN i.ISSUE='25' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney25, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='26' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime26, "
    sql += "  MAX(CASE WHEN i.ISSUE='26' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime26, "
    sql += "  MAX(CASE WHEN i.ISSUE='26' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney26, "
    sql += " MAX(CASE WHEN i.ISSUE='26' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney26, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='27' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime27, "
    sql += "  MAX(CASE WHEN i.ISSUE='27' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime27, "
    sql += "  MAX(CASE WHEN i.ISSUE='27' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney27, "
    sql += " MAX(CASE WHEN i.ISSUE='27' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney27, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='28' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime28, "
    sql += "  MAX(CASE WHEN i.ISSUE='28' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime28, "
    sql += "  MAX(CASE WHEN i.ISSUE='28' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney28, "
    sql += " MAX(CASE WHEN i.ISSUE='28' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney28, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='29' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime29, "
    sql += "  MAX(CASE WHEN i.ISSUE='29' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime29, "
    sql += "  MAX(CASE WHEN i.ISSUE='29' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney29, "
    sql += " MAX(CASE WHEN i.ISSUE='29' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney29, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='30' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime30, "
    sql += "  MAX(CASE WHEN i.ISSUE='30' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime30, "
    sql += "  MAX(CASE WHEN i.ISSUE='30' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney30, "
    sql += " MAX(CASE WHEN i.ISSUE='30' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney30, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='31' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime31, "
    sql += "  MAX(CASE WHEN i.ISSUE='31' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime31, "
    sql += "  MAX(CASE WHEN i.ISSUE='31' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney31, "
    sql += " MAX(CASE WHEN i.ISSUE='31' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney31, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='32' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime32, "
    sql += "  MAX(CASE WHEN i.ISSUE='32' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime32, "
    sql += "  MAX(CASE WHEN i.ISSUE='32' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney32, "
    sql += " MAX(CASE WHEN i.ISSUE='32' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney32, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='33' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime33, "
    sql += "  MAX(CASE WHEN i.ISSUE='33' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime33, "
    sql += "  MAX(CASE WHEN i.ISSUE='33' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney33, "
    sql += " MAX(CASE WHEN i.ISSUE='33' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney33, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='34' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime34, "
    sql += "  MAX(CASE WHEN i.ISSUE='34' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime34, "
    sql += "  MAX(CASE WHEN i.ISSUE='34' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney34, "
    sql += " MAX(CASE WHEN i.ISSUE='34' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney34, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='35' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime35, "
    sql += "  MAX(CASE WHEN i.ISSUE='35' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime35, "
    sql += "  MAX(CASE WHEN i.ISSUE='35' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney35, "
    sql += " MAX(CASE WHEN i.ISSUE='35' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney35, "
    sql += " "
    sql += "  MAX(CASE WHEN i.ISSUE='36' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime36, "
    sql += "  MAX(CASE WHEN i.ISSUE='36' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime36, "
    sql += "  MAX(CASE WHEN i.ISSUE='36' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney36, "
    sql += " MAX(CASE WHEN i.ISSUE='36' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney36, "
    sql += "   "
    sql += "  SUM(i.INVOICE_MONEY) AS invoiceMoneyTotal, "
    sql += "  date_format(now(), '%%m') as invoiceMonthsYear, "
    sql += "  sum(case when i.`INVOICE_TIME` >= unix_timestamp(date_format(now(), '%%Y-01-01'))*1000 then 1 else 0 end) as invoicePeriodsYear "
    sql += "  FROM ICE_DEVICE_RENT_INFO_HISTORY AS h "
    sql += " INNER JOIN ICE_arap AS i ON (h.BODY_NUM=i.BODY_NUM and h.CONTRACT_ID = i.CONTRACT_ID AND i.PAYABLE_ID = h.AGENT_CO_ID)"
    sql += "    AND i.ARAP_STATE=1 and IFNULL(i.PAY_TIME,0)!=0"

    # sql += "  AND i.`RECEIVABLE_ID`='5F8CDA30B99C4841BB236695B21C5E2F' "
    if startTime:
        sql += "       and h.RENT_TIME >= " + str(startTime) + "  "
    if endTime:
        sql += "       and h.RENT_TIME <  " + str(endTime) + " "
    sql += "        "
    sql += "    "
    sql += "    "
    sql += "   "
    # 如果存在参数添加条件
    if deviceModel:
        sql += "        and    h.`DEVICE_MODEL` ='" + deviceModel + "'       "
    if company:
        sql += "        and    h.`AGENT_CO` ='" + company + "'       "
    if keyword:
        sql += "    and  (h.BODY_NUM like '%%" + keyword + "%%' or h.CONTRACT_NO like '%%" + keyword + "%%')"
    sql += "    "
    sql += "  GROUP BY h.contract_id,h.BODY_NUM "
    sql += "  order BY h.`RENT_TIME`"

    rentCostData = pd.read_sql_query(sql, db_ice.conn)

    # 列顺序
    order_col = [
        # 没有 ,
        'signingMonth',
        'agentCo',
        'rentTime',
        'contractNo',
        'customContractNo',
        'deviceModel',
        'period',
        'account',
        # ,
        'bodyNum',
        'deviceLeaseMoney',
        # ,
        'invoiceTime1',
        'payTime1',
        'receiveMoney1',
        'invoiceMoney1',

        'invoiceTime2',
        'payTime2',
        'receiveMoney2',
        'invoiceMoney2',

        'invoiceTime3',
        'payTime3',
        'receiveMoney3',
        'invoiceMoney3',

        'invoiceTime4',
        'payTime4',
        'receiveMoney4',
        'invoiceMoney4',

        'invoiceTime5',
        'payTime5',
        'receiveMoney5',
        'invoiceMoney5',

        'invoiceTime6',
        'payTime6',
        'receiveMoney6',
        'invoiceMoney6',

        'invoiceTime7',
        'payTime7',
        'receiveMoney7',
        'invoiceMoney7',

        'invoiceTime8',
        'payTime8',
        'receiveMoney8',
        'invoiceMoney8',

        'invoiceTime9',
        'payTime9',
        'receiveMoney9',
        'invoiceMoney9',

        'invoiceTime10',
        'payTime10',
        'receiveMoney10',
        'invoiceMoney10',

        'invoiceTime11',
        'payTime11',
        'receiveMoney11',
        'invoiceMoney11',

        'invoiceTime12',
        'payTime12',
        'receiveMoney12',
        'invoiceMoney12',

        'invoiceTime13',
        'payTime13',
        'receiveMoney13',
        'invoiceMoney13',

        'invoiceTime14',
        'payTime14',
        'receiveMoney14',
        'invoiceMoney14',

        'invoiceTime15',
        'payTime15',
        'receiveMoney15',
        'invoiceMoney15',

        'invoiceTime16',
        'payTime16',
        'receiveMoney16',
        'invoiceMoney16',

        'invoiceTime17',
        'payTime17',
        'receiveMoney17',
        'invoiceMoney17',

        'invoiceTime18',
        'payTime18',
        'receiveMoney18',
        'invoiceMoney18',

        'invoiceTime19',
        'payTime19',
        'receiveMoney19',
        'invoiceMoney19',

        'invoiceTime20',
        'payTime20',
        'receiveMoney20',
        'invoiceMoney20',

        'invoiceTime21',
        'payTime21',
        'receiveMoney21',
        'invoiceMoney21',

        'invoiceTime22',
        'payTime22',
        'receiveMoney22',
        'invoiceMoney22',

        'invoiceTime23',
        'payTime23',
        'receiveMoney23',
        'invoiceMoney23',

        'invoiceTime24',
        'payTime24',
        'receiveMoney24',
        'invoiceMoney24',

        'invoiceTime25',
        'payTime25',
        'receiveMoney25',
        'invoiceMoney25',

        'invoiceTime26',
        'payTime26',
        'receiveMoney26',
        'invoiceMoney26',

        'invoiceTime27',
        'payTime27',
        'receiveMoney27',
        'invoiceMoney27',

        'invoiceTime28',
        'payTime28',
        'receiveMoney28',
        'invoiceMoney28',

        'invoiceTime29',
        'payTime29',
        'receiveMoney29',
        'invoiceMoney29',

        'invoiceTime30',
        'payTime30',
        'receiveMoney30',
        'invoiceMoney30',

        'invoiceTime31',
        'payTime31',
        'receiveMoney31',
        'invoiceMoney31',

        'invoiceTime32',
        'payTime32',
        'receiveMoney32',
        'invoiceMoney32',

        'invoiceTime33',
        'payTime33',
        'receiveMoney33',
        'invoiceMoney33',

        'invoiceTime34',
        'payTime34',
        'receiveMoney34',
        'invoiceMoney34',

        'invoiceTime35',
        'payTime35',
        'receiveMoney35',
        'invoiceMoney35',

        'invoiceTime36',
        'payTime36',
        'receiveMoney36',
        'invoiceMoney36',

        'invoiceMoneyTotal',
        'invoiceFinalDate',
        'amountFinalDate',
        # ,
        'yearTicketMonth',
        'YearTicketNumber',
        'itvacost',
        'itvacostAvg',
        # ,
        # ,
        'yearActualCost',
        # ,
        'yearActualCostBuckle',
        #
    ]

    if len(rentCostData) > 0:
        # 合同签订月
        # rentCostData['signingMonth'] = time.strftime("%Y%m", time.localtime(rentCostData['signingDate']/1000))
        # 期间
        # rentCostData['period'] = str(rentCostData['periodNum']//12)+'年'+(str(rentCostData['periodNum']%12) if rentCostData['periodNum']%12 else '')+'期'
        # 开票合计
        # rentCostData['TotalInvoiceMoney'] = rentCostData['InvoiceMoney1'] + rentCostData['InvoiceMoney2'] + rentCostData['InvoiceMoney3'] + rentCostData['InvoiceMoney4'] + rentCostData['InvoiceMoney5'] + rentCostData['InvoiceMoney6'] + rentCostData['InvoiceMoney7'] + rentCostData['InvoiceMoney8'] + rentCostData['InvoiceMoney9'] + rentCostData['InvoiceMoney10'] + rentCostData['InvoiceMoney11'] + rentCostData['InvoiceMoney12'] + rentCostData['InvoiceMoney13'] + rentCostData['InvoiceMoney14'] + rentCostData['InvoiceMoney15'] + rentCostData['InvoiceMoney16'] + rentCostData['InvoiceMoney17'] + rentCostData['InvoiceMoney18'] + rentCostData['InvoiceMoney19'] + rentCostData['InvoiceMoney20'] + rentCostData['InvoiceMoney21'] + rentCostData['InvoiceMoney22'] + rentCostData['InvoiceMoney23'] + rentCostData['InvoiceMoney24'] + rentCostData['InvoiceMoney25'] + rentCostData['InvoiceMoney26'] + rentCostData['InvoiceMoney27'] + rentCostData['InvoiceMoney28'] + rentCostData['InvoiceMoney29'] + rentCostData['InvoiceMoney30'] + rentCostData['InvoiceMoney31'] + rentCostData['InvoiceMoney32'] + rentCostData['InvoiceMoney33'] + rentCostData['InvoiceMoney34'] + rentCostData['InvoiceMoney35'] + rentCostData['InvoiceMoney36']
        # 获取部门数据
        rentCostData['signingMonth'] = rentCostData['signingDate'].apply(
            lambda x: time.strftime("%Y%m", time.localtime(x / 1000)))
        rentCostData['period'] = rentCostData['leaseTerm'].apply(
            lambda x: str(x // 12) + '年' + (str(x if x % 12 else '') + '期'))
        rentCostData['account'] = 1

        # print(rentCostData)
        # print(rentCostData[['period','account']])
        # 添加月份函数
        def datetime_offset_by_month(datetime1, n=1):
            # create a shortcut object for one day
            one_day = datetime.timedelta(days=1)
            # first use div and mod to determine year cycle
            q, r = divmod(datetime1.month + n, 12)
            # create a datetime2
            # to be the last day of the target month
            datetime2 = datetime.datetime(
                datetime1.year + q, r + 1, 1) - one_day
            if datetime1.month != (datetime1 + one_day).month:
                return datetime2
            if datetime1.day >= datetime2.day:
                return datetime2
            return datetime2.replace(day=datetime1.day)

        # 获取invoiceFinalDate
        def get_invoiceFinalDate(parajson):
            x, y = parajson
            x = datetime.datetime.fromtimestamp(x / 1000)
            y = y + 1
            returnData = datetime_offset_by_month(x, y)
            # returnData = int(time.mktime(returnData.timetuple())) * 1000
            returnData = time.strftime("%%Y/%%m/%%d", time.localtime(int(time.mktime(returnData.timetuple()))))
            return returnData

        # 获取invoiceFinalDate
        def get_amountFinalDate(parajson):
            x, y = parajson
            x = datetime.datetime.fromtimestamp(x / 1000)
            returnData = datetime_offset_by_month(x, y)
            # returnData = int(time.mktime(returnData.timetuple())) * 1000
            returnData = time.strftime("%%Y/%%m/%%d", time.localtime(int(time.mktime(returnData.timetuple()))))
            return returnData

        # rentCostData['invoiceFinalDate'] = rentCostData[['signingDate', 'leaseTerm']].apply(get_invoiceFinalDate, axis=1)
        # rentCostData['amountFinalDate'] = rentCostData[['signingDate', 'leaseTerm']].apply(get_amountFinalDate, axis=1)

        # 获取U8上面的数据原值
        sql = "select AVG(itvacost) as itvacost,cinvcode as deviceModel ,cdefine22 as bodyNum,max(cdefine23) as monum,max(cdefine24) as nsnum  from kctranslist group by cinvcode,cdefine22"
        u8CostData = pd.read_sql_query(sql, db_u8.conn)
        # 通过机身号和型号记性合并
        rentCostDatareturn = pd.merge(rentCostData, u8CostData, how='left', on=['deviceModel', 'bodyNum'])
        # 获取平均租机成本
        rentCostDatareturn["itvacostAvg"] = rentCostDatareturn["itvacost"].apply(lambda x: float('%.2f' % (x / 36)))

        # 获取当年实际成本
        def get_yearActualCost(parajson):
            x, y = parajson
            return float("%.2f" % (x * y))

        rentCostDatareturn["yearActualCost"] = rentCostDatareturn[['yearTicketMonth', 'itvacostAvg']].apply(
            get_yearActualCost, axis=1)
        # 获取当年实际成本扣10%
        rentCostDatareturn["yearActualCostBuckle"] = rentCostDatareturn["yearActualCost"].apply(
            lambda x: float('%.2f' % (x * 0.9)))
        # 转中文列明 日期
        # 日期

        # rentCostDatareturn = rentCostDatareturn.ix[:,order_col]
        rentCostDatareturn = rentCostDatareturn.reindex(columns=order_col)
        # rentCostDatareturn = rentCostDatareturn.loc[order_col]
        # rentCostDatareturn = rentCostDatareturn[ order_col ]

        return rentCostDatareturn

    return rentCostData
    # return u8CostData, companyData, deviceModelData, rentCostDatareturn, rentCostData


def get_rent_cost_sql():

    sql = " SELECT "
    sql += " FROM_UNIXTIME(h.SIGNING_DATE/1000 ,'%%Y/%%m') as SIGNING_DATE"
    sql += " ,h.AGENT_CO"
    sql += " ,h.CUSTOMER"
    sql += " ,h.AFTER_SERVICE"
    sql += " ,h.CONTRACT_ID"
    sql += " ,h.CONTRACT_NO"
    sql += " ,h.CUSTOM_CONTRACT_NO"
    sql += " ,h.DEVICE_MODEL"
    sql += " ,h.BODY_NUM"
    sql += " ,FROM_UNIXTIME(h.RENT_TIME/1000,'%%Y/%%m/%%d') as RENT_TIME "
    sql += " ,h.LEASE_TERM "
    sql += " ,h.PERIOD_NUM "
    sql += " ,h.DEVICE_LEASE_MONEY "
    sql += " ,SUM(i.INVOICE_MONEY) "
    for n in range(1, 37):
        n = str(n)
        sql += " ,MAX(CASE WHEN i.ISSUE='" + n + "' and i.ARAP_STATE ='1' THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) PAID_TIME" + n
        sql += " ,MAX(CASE WHEN i.ISSUE='" + n + "' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) INVOICE_TIME" + n
        sql += " ,MAX(CASE WHEN i.ISSUE='" + n + "' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) INVOICE_MONEY" + n
        sql += " ,MAX(CASE WHEN i.ISSUE='" + n + "' and i.ARAP_STATE ='1'  and i.AMOUNT_MONEY is not null  THEN i.`AMOUNT_MONEY` ELSE NULL END ) AMOUNT_MONEY" + n

    sql += " ,SUM(case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then 1 else 0 end) as  CUR_YEAR_INVOICE_NUM "
    sql += " ,COUNT(distinct (case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then month(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) else 0 end)) as  CUR_YEAR_INVOICE_MONTHS "

    sql += " FROM ice_device_rent_info_history h LEFT JOIN ice_arap i on h.CONTRACT_ID = i.CONTRACT_ID"

    sql += " WHERE h.CONTRACT_TYPE!=2 "
    sql += " AND i.ARAP_STATE=1 and IFNULL(i.PAY_TIME,0)!=0 "
    sql += " GROUP BY h.SIGNING_DATE,h.AGENT_CO,h.CONTRACT_NO,h.CUSTOM_CONTRACT_NO,h.DEVICE_MODEL,h.LEASE_TERM,h.BODY_NUM,h.PERIOD_NUM,h.DEVICE_LEASE_MONEY,h.RENT_TIME "
    sql += " ,h.CUSTOMER"
    sql += " ,h.AFTER_SERVICE"
    sql += " ,h.CONTRACT_ID"

    return sql


def get_rent_cost_data():
    sql = get_rent_cost_sql()
    # print(sql)
    data = pd.read_sql_query(sql, db_ice.conn)

    data['period'] = data['LEASE_TERM'].apply(
        lambda x: str(x // 12) + '年' + (str(x if x % 12 else '') + '期'))

    # 获取U8上面的数据原值
    sql = "select cinvcode as DEVICE_MODEL,cdefine22 as BODY_NUM, AVG(itvacost) as itvacost, max(cdefine23) as monum, max(cdefine24) as nsnum  from kctranslist group by cinvcode,cdefine22"
    u8_cost_data = pd.read_sql_query(sql, db_u8.conn)

    # 通过机身号和型号记性合并
    new_data = pd.merge(data, u8_cost_data, how='left', on=['DEVICE_MODEL', 'BODY_NUM'])
    #
    # # 获取平均租机成本
    new_data["itvacost_avg"] = new_data["itvacost"].apply(lambda x: float('%.2f' % (x / 36)))

    # 获取当年实际成本
    def get_year_actual_cost(inp):
        x, y = inp
        return float("%.2f" % (x * y))

    new_data["yearActualCost"] = new_data[['CUR_YEAR_INVOICE_MONTHS', 'itvacost_avg']].apply(get_year_actual_cost, axis=1)
    # 获取当年实际成本扣10%
    new_data["yearActualCostBuckle"] = new_data["yearActualCost"].apply(
        lambda x: float('%.2f' % (x * 0.9)))

    return new_data


def main():
    # sql = get_rent_cost_sql()
    # print(sql)
    print('------ main start -------')
    # 从u8上获取数据

    data = get_rent_cost_data()

    # 把数据插入大数据服务器的数据库
    db_big_data.insert_data(data, 'fin_rent_cost', if_exists='replace')

    # print("------ ice_device_rent_info_history -------")
    # data = pd.read_sql_query("select * from ice_device_rent_info_history", db_ice.conn)
    # db_big_data.insert_data(data, 'ice_device_rent_info_history', if_exists='replace')
    # print("------ ice_arap -------")
    # data = pd.read_sql_query("select * from ice_arap", db_ice.conn)
    # db_big_data.insert_data(data, 'ice_arap', if_exists='replace')
    # print("------ Main End -------")


if __name__ == "__main__":
    main()
