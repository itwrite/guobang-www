#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License") you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import concurrent.futures
import datetime
import time
import os
import os.path
import math

import pandas as pd
import sqlalchemy
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from dateutil.relativedelta import relativedelta
# dictData
from pandas import DataFrame
from tornado.options import define, options


#import common functions
from common.functions import *

#i-synergy 目标数据库（Mysql）
from common import connector

conn = sqlalchemy.create_engine("mssql+pymssql://icereader:ice789@116.6.132.90:1433/UFDATA_198_2011?charset=utf8")

ice_conn = connector.db().conn

define("port", default=8899, help="run on the given port", type=int)

# A thread pool to be used for password hashing with bcrypt.
executor = concurrent.futures.ThreadPoolExecutor(2)

_d_now_date = datetime.datetime.now()
#
# =========================================================================
# =========================================================================
#

def filterFields(fields, dict_data):
    if fields == '*':
        return dict_data
    result = {}
    if isinstance(dict_data, dict):
        if isinstance(fields, list):
            for f in fields:
                if isinstance(f, (list, dict)):
                    sub_dict = filterFields(f, dict_data)
                    for k in sub_dict:
                        result[k] = sub_dict[k]
                elif isinstance(f, str) and f in dict_data:
                    result[f] = dict_data[f]
                else:
                    result[f] = ""
        elif isinstance(fields, dict):
            for f in fields:
                result[f] = fields[f]
        elif isinstance(fields, str):
            if fields in dict_data:
                result[fields] = dict_data[fields]
    return result


def dictMerge(x, y):
    z = x.copy()
    z.update(y)
    return z


def dictToSql(dict_data):
    arr = []
    for key, value in dict_data.items():
        if value.strip() == '':
            arr.append(key)
        else:
            arr.append(key + " AS " + value)
    return ",".join(arr)


def getFieldsSql(fields, dict_data):
    dict_data = filterFields(fields, dict_data)
    return dictToSql(dict_data)


#
# =========================================================================
# =========================================================================
#

def sumCostForDept(cDepCode, ccodes, _startDate, _endDate):
    _startDate = _startDate.strftime("%Y-%m-%d") if isinstance(_startDate, datetime.datetime) else _startDate
    _endDate = _endDate.strftime("%Y-%m-%d") if isinstance(_endDate, datetime.datetime) else _endDate

    main_sql = "SELECT ga.ccode,sum(ga.mc) as total_mc,sum(ga.md) as total_md"
    main_sql += " FROM GL_accvouch ga"
    main_sql += " WHERE (ga.dbill_date between '%s' and '%s') and ga.cdept_id='%s' and ga.ccode in('%s')" % (
        _startDate, _endDate, cDepCode, "','".join(ccodes))
    main_sql += " GROUP by ga.ccode"
    # print(main_sql)
    result = pd.read_sql_query(main_sql, conn)
    y = 0
    for i, row in result.iterrows():
        y += row['total_mc']
    return y


def getInitialBalanceAmount(cDepCode, ccodes, _startDate):
    _startDate = _startDate.strftime("%Y-%m-%d") if isinstance(_startDate, datetime.datetime) else _startDate
    main_sql = "SELECT ga.ccode,sum(ga.mc) as total_mc,sum(ga.md) as total_md"
    main_sql += " FROM GL_accvouch ga"
    main_sql += " WHERE ga.dbill_date < '%s' and ga.cdept_id='%s' and ga.ccode in('%s')" % (
        _startDate, cDepCode, "','".join(ccodes))
    main_sql += " GROUP by ga.ccode"
    result = pd.read_sql_query(main_sql, conn)
    y = 0
    for i, row in result.iterrows():
        y += row['total_mc']
    return y


def getDepartment(cDepCode):
    sql = "select * from Department where cDepCode='%s'" % cDepCode
    return pd.read_sql_query(sql, conn)


def getAllDepartments():
    sql = "select * from Department"
    return pd.read_sql_query(sql, conn)


def getCodesForDeptDf(iyear):
    cclass_lst = ['资产', '负债', '共同', '权益', '成本', '损益']
    iyear = iyear.strftime("%Y") if isinstance(iyear, datetime.datetime) else iyear
    sql = "SELECT c.ccode_name,c.ccode from Code c where c.iyear='%s' and bdept=1 and cclass in ('%s')" % (iyear, "','".join(cclass_lst))
    return pd.read_sql_query(sql, conn)


def getCodesForPersonDf(iyear):
    cclass_lst = ['资产', '负债', '损益']  # ['资产','负债','共同','权益','成本','损益']
    iyear = iyear.strftime("%Y") if isinstance(iyear, datetime.datetime) else iyear
    sql = "SELECT c.ccode_name,c.ccode,c.igrade from Code c where c.iyear='%s' and bperson=1 and cclass in ('%s')" % (
    iyear, "','".join(cclass_lst))
    #     print(sql)
    return pd.read_sql_query(sql, conn)


global_sqls = []


def sumCostForPerson(cPersonCode, ccodes, _startDate, _endDate):
    _startDate = _startDate.strftime("%Y-%m-%d") if isinstance(_startDate, datetime.datetime) else _startDate
    _endDate = _endDate.strftime("%Y-%m-%d") if isinstance(_endDate, datetime.datetime) else _endDate

    main_sql = "SELECT ga.ccode,sum(ga.mc) as total_mc,sum(ga.md) as total_md"
    main_sql += " FROM GL_accvouch ga"
    main_sql += " WHERE (ga.dbill_date between '%s' and '%s') and ga.cperson_id='%s' and ga.ccode in('%s')" % (
    _startDate, _endDate, cPersonCode, "','".join(ccodes))
    main_sql += " GROUP by ga.ccode"
    # print(main_sql)

    result = pd.read_sql_query(main_sql, conn)
    y = 0
    for i, row in result.iterrows():
        y += row['total_mc']

    if y > 0:
        global_sqls.append(main_sql)
    return y


def getGlAccvouchDf(_startDate, _endDate):
    _startDate = _startDate.strftime("%Y-%m-%d") if isinstance(_startDate, datetime.datetime) else _startDate
    _endDate = _endDate.strftime("%Y-%m-%d") if isinstance(_endDate, datetime.datetime) else _endDate

    main_sql = "SELECT ga.ccode,ga.mc,ga.md,ga.cperson_id,ga.cdept_id"
    main_sql += " FROM GL_accvouch ga"
    main_sql += " WHERE (ga.dbill_date between '%s' and '%s')" % (_startDate, _endDate)
    # print(main_sql)

    return pd.read_sql_query(main_sql, conn)


def sumAccvoucMc(df):
    data = df.groupby(by=['ccode']).agg({'mc': sum})
    y = 0
    for i, row in data.iterrows():
        y += row['mc']
    return y


def getPersonsDf(cDepCode):
    sql = "select p.cPersonCode,p.cPersonName,p.cDepCode from Person p where p.cDepCode = '%s'" % (cDepCode)
    return pd.read_sql_query(sql, conn)


def getMonthDelta(first_date, second_date):
    delta = second_date - first_date
    months = []
    for d in range(1, delta.days):
        _d_date = (first_date + relativedelta(days=d))
        m = _d_date.strftime('%Y-%m')
        if m not in months:
            months.append(m)
    return len(months)

# 找出当前科目的父科目
def getExceptParentCodes(ccode, data):
    row = data.loc[data.ccode == ccode]

    for k, n in row.iterrows():
        if n['igrade'] > 1:
            p_len = 2 * n['igrade']
            p_ccode = ccode[0:p_len]
            return data.loc[data.ccode != p_ccode]
    return data
##
## =========================================================================
## =========================================================================
##

#
# Application
#
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", HomeHandler),
            (r"/person_cost", PersonCostHandler),
            (r"/accounts_payable", AccountsPayableHandler),
            (r"/temporary_estimation",TemporaryEstimationHandler),
            (r"/capitalized_cost",CapitalizedCostHandler),
            (r"/capitalized_cost_by_class", CapitalizedCostByClassHandler),
            (r"/ice_rent_cost", IceRentCostHandler)
        ]
        settings = dict(
            web_title=u"国邦",
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=True,
            cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
            debug=True,
        )
        super(Application, self).__init__(handlers, **settings)


#
# BaseHandler
#
class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return 0


class HomeHandler(BaseHandler):

    def get(self):

        _depCode = self.get_argument("cDepCode", 'K')  # 售后服务支持部
        _d_now_month = _d_now_date.strftime('%Y-%m')
        if _depCode == '':
            _depCode = 'K'

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _s_date_format = '%Y-%m-%d'

        _i_period_months = 1
        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')
        _d_endDate = (_d_endDate + relativedelta(months=1))

        # test
        # sumCostForDept("1",['660201'],_d_startDate,_d_endDate)
        # codes_df = getCodesForDeptDf(_d_startDate.strftime("%Y"))
        # ccodeNames = codes_df[codes_df['ccode_name']=='其他']

        # result_list['cPersonName'] = result_list[['cPersonName','cDepName']].apply(lambda x:x['cPersonName']+'('+x['cDepName']+')',axis=1)
        # result_list

        # 找出需要统计的科目
        # 部门的科目分类为“损益”

        # 获取所有部门
        all_department_df = getAllDepartments()
        department_df = all_department_df[all_department_df['cDepCode'] == _depCode]

        # 获取需要统计的科目
        codes_df = getCodesForDeptDf(_d_startDate.strftime("%Y"))

        ccode_names = codes_df['ccode_name'].drop_duplicates()
        # 初始化
        dictData = {'科目': []}
        columns = ['科目']
        n = 0
        for i, row in department_df.iterrows():

            # ==================
            # ==================
            cDepName = row['cDepName']
            for j in range(0, getMonthDelta(_d_startDate, _d_endDate)):

                startDate = (_d_startDate + relativedelta(months=j))
                endDate = (startDate + relativedelta(months=1))
                _extfix = "____(" + (_d_startDate + relativedelta(months=j)).strftime("%Y-%m") + ")____"
                if _extfix not in dictData:
                    dictData[cDepName + _extfix] = []
                    columns.append(cDepName + _extfix)

                total_cost = 0
                for code_name in ccode_names:
                    if n == 0:
                        dictData['科目'].append(code_name)
                    codeNames = codes_df[codes_df['ccode_name'] == code_name]
                    cost = sumCostForDept(row['cDepCode'], codeNames['ccode'], startDate, endDate)
                    dictData[cDepName + _extfix].append(cost)
                    total_cost += cost
                # 合计
                if n == 0:
                    dictData['科目'].append("合计")
                dictData[cDepName + _extfix].append(round(total_cost, 2))
                n += 1

        data = DataFrame(dictData,columns=columns)

        self.render("department_cost.html", listData=data,
                    department_df=department_df,
                    all_department_df=all_department_df,
                    _depCode=_depCode,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


class PersonCostHandler(BaseHandler):

    def get(self):

        _depCode = self.get_argument("cDepCode", '')  # 售后服务支持部
        _d_now_month = _d_now_date.strftime('%Y-%m')
        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _s_date_format = '%Y-%m-%d'

        _i_period_months = 1
        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')
        _d_endDate = (_d_endDate + relativedelta(months=1))

        # sumCostForPerson("094", ['66010801'], _d_startDate, _d_endDate)
        # codes_df = getCodesForPersonDf(_d_startDate.strftime("%Y"))
        # ccodeNames = codes_df[codes_df['ccode_name']=='其他']
        # ccodeNames
        # codes_df['ccode']
        # person_df = getPersonsDf(_depCode)
        # person_df

        # 获取所有部门
        all_department_df = getAllDepartments()
        department_df = all_department_df[all_department_df['cDepCode'] == _depCode]
        if _depCode == '':
            department_df = all_department_df

        # 获取所有数据
        all_accvouch_df = getGlAccvouchDf(_d_startDate, _d_endDate)

        # 获取需要统计的科目
        codes_df = getCodesForPersonDf(_d_startDate.strftime("%Y"))

        for row in codes_df.iterrows():
            codes_df = getExceptParentCodes(row[1]['ccode'], codes_df)

        ccode_names = codes_df['ccode_name'].drop_duplicates()

        # 初始化
        data = {'科目': []}

        columns = ['科目']
        n = 0
        for i, dept in department_df.iterrows():
            d_name = dept['cDepName']
            person_df = getPersonsDf(dept['cDepCode'])
            for j, person in person_df.iterrows():
                p_name = person['cPersonName']
                key = '____(' + d_name + ')____' + p_name
                if key not in data:
                    data[key] = []
                    columns.append(key)

                total_cost = 0

                for c_name in ccode_names:
                    if n == 0:
                        data['科目'].append(c_name)
                    codes = codes_df[codes_df['ccode_name'] == c_name]
                    vouchs = all_accvouch_df[all_accvouch_df['cperson_id'] == person['cPersonCode']]
                    vouchs = vouchs[vouchs['ccode'].isin(codes['ccode']) == True]
                    cost = sumAccvoucMc(vouchs)
                    #             cost = sumCostForPerson(person['cPersonCode'], codes['ccode'], _d_startDate, _d_endDate)
                    total_cost += cost
                    data[key].append(float('%.2f' % cost))
                # 合计
                if n == 0:
                    data['科目'].append('合计')
                data[key].append(float('%.2f' % total_cost))
                n += 1

        data = DataFrame(data,columns=columns)

        self.render("person_cost.html",
                    listData=data,
                    department_df=department_df,
                    all_department_df=all_department_df,
                    _depCode=_depCode,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


from tables.Code import *
from tables.Vendor import *
from tables.GL_accvouch import *


##获取初期余额
def get_balance(venCode, limitDate):
    # 贷方金额 为 已收到票据但未付款
    sql = "select distinct ga.i_id, v.cVenName as 供应商名称,ga.mc AS 贷方金额, ga.md as 借方金额 from GL_accvouch ga \
    LEFT JOIN Vendor v ON(v.cVenCode = ga.csup_id) \
    LEFT JOIN Code c ON(c.ccode = ga.ccode) \
    where ga.csup_id='%s' and c.ccode_name='人民币' and ga.dbill_date < '%s' " % (venCode, limitDate)
    res = pd.read_sql_query(sql, conn)
    res = res.groupby(by=['供应商名称']).agg({'借方金额': sum, '贷方金额': sum})
    total_amount = 0
    for row in res.iterrows():
        total_amount = total_amount + (row[1]['贷方金额'] - row[1]['借方金额'])
    return total_amount


# 根据年份获取所有科目
def getCodes(year):
    if isinstance(year,list):
        year = "','".join(year)
    sql = "select c.i_id,c.ccode,c.cclass,c.igrade,c.ccode_name,c.bend from Code c where iyear in('%s') " % year
    result = pd.read_sql_query(sql, conn)
    return result


# 找出当前科目的父科目
def getParentCode(ccode, data):
    row = data[data.ccode == ccode]

    for k, n in row.iterrows():
        if n['igrade'] > 1:
            p_len = 2 * n['igrade']
            p_ccode = ccode[0:p_len]
            return data[data.ccode == p_ccode]
    return row


# 提取所有科目的名称
def getCodeName(code):
    for k, n in code.iterrows():
        return n['ccode_name']



class AccountsPayableHandler(BaseHandler):
    def get(self):
        _cCode = self.get_argument("_cCode", '')
        _d_now_month = _d_now_date.strftime('%Y-%m')
        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _s_date_format = '%Y-%m-%d'

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')
        _d_endDate = (_d_endDate + relativedelta(months=1))

        # _cCode = "220202"  # 人民币

        _yearsList = []

        # 提取出所需要统计的年份，可能跨年份
        for i in range(0, getMonthDelta(_d_startDate, _d_endDate)):
            iyear = (_d_startDate + relativedelta(months=i)).strftime("%Y")
            if iyear not in _yearsList:
                _yearsList.append(iyear)

        #合并字段
        dict_fields = dictMerge(GL_accvouch, Code)
        dict_fields = dictMerge(dict_fields, Vendor)

        # #拼接SQL
        fields_sql = getFieldsSql(
            [{'left(convert(varchar, ga.dbill_date, 120),10)':'制单日期'}, 'ga.ccode', 'c.ccode_name', 'ga.cdigest', 'ga.md', 'ga.mc', 'v.cVenCode', 'v.cVenName'],
            dict_fields)
        # main_sql = fields_sql
        main_sql = "SELECT (ga.csign+'-'+CONVERT(varchar(10), ga.ino_id)) as 凭证号, %s" % fields_sql
        main_sql += " FROM GL_accvouch ga "
        main_sql += " JOIN Code c ON(c.ccode = ga.ccode and c.iyear in ('%s') and c.ccode='%s') " % ("','".join(_yearsList), _cCode)
        main_sql += " JOIN Vendor v ON(v.cVenCode = ga.csup_id) "
        # main_sql+= " JOIN SettleStyle ss ON(ss.cSSCode = ga.csettle)"
        main_sql += " WHERE (ga.dbill_date between '%s' and '%s')" % (_d_startDate.strftime(_s_date_format), _d_endDate.strftime(_s_date_format))  # c.ccode_name='人民币' and
        main_sql += " ORDER BY ga.dbill_date"
        #
        # #从数据库里取数据
        result = pd.read_sql_query(main_sql, conn)

        #获取相应的科目
        codes_data = getCodes(_yearsList)
        #
        vendor_codes = result['供应商编码'].drop_duplicates()

        data = {}
        for col in result.columns:
            data[col] = []
        data['余额'] = []
        data['-'] = []
        for venCode in vendor_codes:
            new_data = result.loc[(result["供应商编码"] == venCode)]

            total_balance = get_balance(venCode, _d_startDate.strftime(_s_date_format))
            i = 0
            lend_total = 0
            borrow_total = 0
            for index, row in new_data.iterrows():
                if i == 0:
                    for col in result.columns:
                        if col in data:
                            if col == '摘要':
                                data[col].append('初期余额')
                            else:
                                data[col].append('')
                    data['余额'].append(round(total_balance,2))
                    data['-'].append('')
                for col in result.columns:
                    if col in data:
                        data[col].append(row[col])
                lend_total = lend_total + row['贷方金额']
                borrow_total = borrow_total + row['借方金额']
                total_balance = total_balance + row['贷方金额'] - row['借方金额']
                data['余额'].append(round(total_balance,2))
                data['-'].append(getCodeName(getParentCode(row['科目编码'], codes_data)) + '-' + row['科目名称'])
                #         print(lend_total)
                if i == len(new_data) - 1:
                    for col in result.columns:

                        if col in data:
                            if col == '摘要':
                                data[col].append('小计')
                            elif col == '贷方金额':
                                data[col].append(str(round(lend_total, 2)))
                            elif col == '借方金额':
                                data[col].append(str(round(borrow_total, 2)))
                            else:
                                data[col].append(' ')
                        data[col].append('')
                    data['余额'].append(str(round(total_balance, 2)))
                    data['余额'].append('')

                    data['-'].append('')
                    data['-'].append('-')

                i = i + 1

        data = DataFrame(data)

        self.render("accounts_payable.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date,
                    main_sql=main_sql)

class TemporaryEstimationHandler(BaseHandler):
    def get(self):
        sql = " select  "
        sql += " rd.ID"
        sql += " ,po.cVenPUOMProtocol   AS 收付款协议编码 "
        sql += " ,agr.cName   AS 收付款协议 "
        sql += " ,rd.cOrderCode  AS 订单号 "
        sql += " ,rds.dSDate   AS 结算日期 "
        sql += " ,wh.cWhCode   AS 仓库编码 "
        sql += " ,wh.cWhName   AS 仓库 "
        sql += " ,CONVERT(varchar(100),rd.dDate , 23)   AS 单据日期 "
        sql += " ,rds.cInVouchCode AS 入库单号 "
        sql += " ,ven.cVenName  AS 供应商 "
        sql += " ,rds.cInvCode  AS 存货编码 "
        sql += " ,iv.cInvName  AS 存货编码 "
        sql += " ,iv.cInvStd   AS 规格型号 "
        sql += " ,cu.cComUnitName    AS 主计量单位 "
        sql += " ,rds.iQuantity  AS 数量 "
        sql += " ,rds.iOriTaxCost AS 原币含税单价 "
        sql += " ,rds.iPrice   AS 原币金额 "
        sql += " ,rds.iOriTaxPrice AS 原币税额 "
        sql += " ,rds.iTaxRate  AS 税率 "
        sql += " ,rds.iTaxPrice  AS 本币税额 "
        sql += " ,rds.iSum   AS 本币价税合计 "
        sql += " ,rd.cExch_Name  AS 币种名称 "
        sql += " ,ISNULL(rds.iMaterialFee,0) AS 材料费 "
        sql += " ,ISNULL(rds.iProcessFee,0) AS 加工费单价 "
        sql += " ,ISNULL(rds.iProcessFee,0)*rds.iQuantity as 加工费 "
        sql += " , (case when wh.cWhName='委外仓' then isnull(rds.iProcessFee,0) else isnull(rds.iAPrice,0) end) as 暂估金额 "
        sql += "  "
        sql += " from RdRecord01 rd "
        sql += "  LEFT JOIN RdRecords01 rds ON(rds.ID = rd.ID) "
        sql += "  LEFT JOIN Warehouse wh ON(rd.cWhCode = wh.cWhCode) "
        sql += "  LEFT JOIN Vendor ven ON(ven.cVenCode = rd.cVenCode) "
        sql += "  LEFT JOIN PO_Pomain po on po.cPOID = rd.cOrderCode "
        sql += "  LEFT JOIN PO_Podetails pod on po.cPOID = pod.cPOID "
        sql += "  LEFT JOIN Inventory  iv on iv.cInvCode=rds.cInvCode "
        sql += "  LEFT JOIN ComputationUnit cu on iv.cComUnitCode = cu.cComunitCode "
        sql += "  LEFT JOIN AA_Agreement agr on agr.cCode = po.cVenPUOMProtocol "

        sql += " WHERE "
        sql += "  wh.cWhName!='资产仓'"
        sql += "  and ISNULL(rds.dSDate,0)=0 and ISNULL(rd.dkeepdate,0)=0"
        sql += " ORDER BY rd.dDate DESC "
        main_sql = sql

        result = pd.read_sql_query(main_sql, conn)
        result.fillna('',inplace= True)

        paylist = {
            '预付货款': 0,
            '货到付款': 0,
            '月结30天': 30,
            '月结60天': 60,
            '月结15天': 15,
            '月结45天': 45,
            '现金': 0
        }

        # now try to calculate pay date
        def paydate(inp):
            invdate = inp['单据日期']
            paymode = inp['收付款协议']

            if invdate != '':
                _d_start_date = datetime.datetime.strptime(invdate, '%Y-%m-%d')

                d_delta = paylist[paymode] if paymode in paylist else 0

                invdate = _d_start_date + datetime.timedelta(d_delta)

                if invdate.day > 15:
                    invdate += datetime.timedelta(16)
                    invdate = datetime.date(invdate.year, invdate.month, 15)
                    return invdate.strftime('%Y-%m-%d')
                else:
                    invdate = datetime.date(invdate.year, invdate.month, 15)
                    return invdate.strftime('%Y-%m-%d')
            return invdate

        # calculate the payday
        result['预计日期'] = result[['单据日期', '收付款协议']].apply(paydate, axis=1)

        result['收付款协议'] = result['收付款协议'].apply(lambda x: '委外' if x == '' else x)

        cols = ["供应商",'付款方式', '预计日期','外购', '委外', '价税合计', '预付', '到付']
        data = {}
        for v in cols:
            data[v] = []
        for row in result.iterrows():
            data['供应商'].append(row[1]['供应商'])
            data['价税合计'].append(row[1]['本币价税合计'])
            data['付款方式'].append(row[1]['收付款协议'])
            data['预计日期'].append(row[1]['预计日期'])

            if row[1]['收付款协议'] == '':
                data['委外'].append(row[1]['加工费'] + (1 + row[1]['税率'] / 100))
                data['外购'].append(0)
            else:
                data['外购'].append(row[1]['暂估金额'])
                data['委外'].append(0)

            if row[1]['收付款协议'] == '预付货款':
                data['预付'].append(row[1]['暂估金额'])
                data['到付'].append(0)
            else:
                data['预付'].append(0)
                data['到付'].append(row[1]['暂估金额'])

        data = DataFrame(data)


        # self.render("accounts_payable.html")
        self.render("temporary_estimation.html",
                    listData=data
                    )


def getCapitalizedCostSQL(start_date,end_date):
    sql = "  SELECT  "
    sql += "  dl.cDLCode AS 发退货单号  "
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
    sql += "  ,sod.iFHMoney AS 开票金额  "
    sql += "  ,(dls.iQuantity - dls.fVeriBillQty) AS 未开票数量  "
    sql += "  ,(dls.iQuantity - dls.fVeriBillQty)*iass.iOutCost AS 未开票金额  "
    sql += "  ,(dls.fVeriBillSum) AS 回款金额  "
    sql += "  ,(dls.iSettleNum - dls.fVeriBillSum) AS 未回款金额  "
    sql += "  ,dls.iQuantity AS 成本单价数量  "
    sql += "  ,iass.iOutCost AS 成本单价  "
    sql += "  ,(iass.iOutCost*dls.iQuantity) AS 成本金额  "
    sql += "  ,(dls.iNatMoney-iass.iOutCost*dls.iQuantity) AS 发货毛利  "
    sql += "  ,(dls.fVeriBillSum-iass.iOutCost*dls.fVeriBillQty) AS 发货回款毛利  "
    sql += "  ,dl.dDate AS 日期  "
    sql += "  ,LEFT(CONVERT(varchar(100),dl.dDate, 23),7) AS 月份  "
    sql += "  ,iass.iOutCost AS 材料成本单价  "
    sql += "  ,(iass.iOutCost * dls.iQuantity) AS 销售材料成本  "
    sql += "  ,dls.cInvName AS 产品分类  "
    sql += "  ,(case when cus.bCusOverseas = 1 then '国外' else '国内' end) AS 销售区域  "
    sql += "  ,(case when cus.bCusOverseas = 1 then '国外' else '国内' end) AS 国外国内  "

    sql += "  FROM DispatchList dl   "
    sql += "  JOIN DispatchLists dls ON(dl.DLID = dls.DLID)   "
    sql += "  JOIN Customer cus ON(cus.cCusCode = dl.cCusCode)   "
    sql += "  JOIN SO_SODetails sod ON(sod.iSOsID = dls.iSOsID)   "
    sql += "  JOIN IA_Subsidiary iass ON(iass.isaleordersid = sod.AutoID)   "
    sql += "  WHERE   "
    sql += "  dl.cCusCode !='' "
    sql += "  and (dl.dDate between '%s' and '%s')   " % (start_date,end_date)

    return sql


class CapitalizedCostHandler(BaseHandler):

    def get(self):

        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _s_date_format = '%Y-%m-%d'

        _i_period_months = 1
        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')
        # _d_endDate = (_d_endDate + relativedelta(months=1))


        # sql += "  and dl.cDLCode='171113376'  "
        main_sql = getCapitalizedCostSQL(_d_startDate.strftime('%Y-%m-01'), _d_endDate.strftime('%Y-%m-01'))

        data = pd.read_sql_query(main_sql, conn)

        data.groupby(by=['存货分类','存货编码','存货名称']).agg({'发货数量':sum,'发货含税金额':sum,'销售材料成本':sum,'发货无税金额':sum,'发货毛利':sum}).reset_index()
        self.render("capitalized_cost.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


class CapitalizedCostByClassHandler(BaseHandler):

    def get(self):


        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _s_date_format = '%Y-%m-%d'

        _i_period_months = 1
        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')
        # _d_endDate = (_d_endDate + relativedelta(months=1))


        # sql += "  and dl.cDLCode='171113376'  "
        main_sql = getCapitalizedCostSQL(_d_startDate.strftime('%Y-%m-01'), _d_endDate.strftime('%Y-%m-01'))

        data = pd.read_sql_query(main_sql, conn)

        classes = pd.read_csv(os.path.dirname(os.path.realpath(__file__))+'/tables/CunHuoDangAn-class-20171018.csv')


        def getClassName(inp):

            _classes = classes[classes['存货编码'] == inp['存货编码']]
            name = '其他'
            if len(_classes) == 1:
                for row in _classes.iterrows():
                    return row[1]['一级类别'] if isinstance(row[1]['二级类别'], float) and math.isnan(row[1]['二级类别']) else \
                    row[1]['二级类别']
            return name

        data['存货分类'] = data[['存货编码']].apply(getClassName, axis=1)

        self.render("capitalized_cost_by_class.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


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
    sql += "          h.`SIGNING_DATE` as signingDate,h.`AGENT_CO` as agentCo,h.`CONTRACT_NO` as "
    sql += "          contractNo,h.contract_id as contractId, "
    sql += "          h.`CUSTOM_CONTRACT_NO` as customContractNo,h.`DEVICE_MODEL` as deviceModel,h.`BODY_NUM` AS "
    sql += "          bodyNum, "
    sql += "          h.`RENT_TIME` as rentTime,h.`RETU_TIME` as retuTime,h.LEASE_TERM as "
    sql += "          leaseTerm,h.PERIOD_NUM AS periodNum, "
    sql += "          h.DEVICE_LEASE_MONEY as deviceLeaseMoney, "
    sql += "          count(distinct (case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then month(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) else null end)) as  YearTicketMonth, "
    sql += "          sum(case when year(now())=year(FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d')) then 1 else 0 end) as  YearTicketNumber, "
    sql += "          MAX(CASE WHEN i.ISSUE='1' and i.ARAP_STATE ='1' THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime1, "
    sql += "          MAX(CASE WHEN i.ISSUE='1' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime1, "
    sql += "          MAX(CASE WHEN i.ISSUE='1' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney1, "
    sql += "         MAX(CASE WHEN i.ISSUE='1' and i.ARAP_STATE ='1'  and i.AMOUNT_MONEY is not null  THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney1, "
    sql += "           "
    sql += "          MAX(CASE WHEN i.ISSUE='2' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime2, "
    sql += "          MAX(CASE WHEN i.ISSUE='2' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime2, "
    sql += "          MAX(CASE WHEN i.ISSUE='2' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney2, "
    sql += "         MAX(CASE WHEN i.ISSUE='2'and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null  THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney2, "
    sql += "           "
    sql += "          MAX(CASE WHEN i.ISSUE='3' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime3, "
    sql += "          MAX(CASE WHEN i.ISSUE='3' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime3, "
    sql += "          MAX(CASE WHEN i.ISSUE='3' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney3, "
    sql += "         MAX(CASE WHEN i.ISSUE='3' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney3, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='4' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime4, "
    sql += "          MAX(CASE WHEN i.ISSUE='4' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime4, "
    sql += "          MAX(CASE WHEN i.ISSUE='4' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney4, "
    sql += "         MAX(CASE WHEN i.ISSUE='4' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney4, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='5' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime5, "
    sql += "          MAX(CASE WHEN i.ISSUE='5' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime5, "
    sql += "          MAX(CASE WHEN i.ISSUE='5' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney5, "
    sql += "         MAX(CASE WHEN i.ISSUE='5' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney5, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='6' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime6, "
    sql += "          MAX(CASE WHEN i.ISSUE='6' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime6, "
    sql += "          MAX(CASE WHEN i.ISSUE='6' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney6, "
    sql += "         MAX(CASE WHEN i.ISSUE='6' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney6, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='7' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime7, "
    sql += "          MAX(CASE WHEN i.ISSUE='7' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime7, "
    sql += "          MAX(CASE WHEN i.ISSUE='7' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney7, "
    sql += "         MAX(CASE WHEN i.ISSUE='7' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney7, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='8' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime8, "
    sql += "          MAX(CASE WHEN i.ISSUE='8' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime8, "
    sql += "          MAX(CASE WHEN i.ISSUE='8' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney8, "
    sql += "         MAX(CASE WHEN i.ISSUE='8' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney8, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='9' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime9, "
    sql += "          MAX(CASE WHEN i.ISSUE='9' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime9, "
    sql += "          MAX(CASE WHEN i.ISSUE='9' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney9, "
    sql += "         MAX(CASE WHEN i.ISSUE='9' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney9, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='10' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime10, "
    sql += "          MAX(CASE WHEN i.ISSUE='10' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime10, "
    sql += "          MAX(CASE WHEN i.ISSUE='10' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney10, "
    sql += "         MAX(CASE WHEN i.ISSUE='10' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney10, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='11' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime11, "
    sql += "          MAX(CASE WHEN i.ISSUE='11' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime11, "
    sql += "          MAX(CASE WHEN i.ISSUE='11' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney11, "
    sql += "         MAX(CASE WHEN i.ISSUE='11' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney11, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='12' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime12, "
    sql += "          MAX(CASE WHEN i.ISSUE='12' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime12, "
    sql += "          MAX(CASE WHEN i.ISSUE='12' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney12, "
    sql += "         MAX(CASE WHEN i.ISSUE='12' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney12, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='13' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime13, "
    sql += "          MAX(CASE WHEN i.ISSUE='13' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime13, "
    sql += "          MAX(CASE WHEN i.ISSUE='13' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney13, "
    sql += "         MAX(CASE WHEN i.ISSUE='13' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney13, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='14' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime14, "
    sql += "          MAX(CASE WHEN i.ISSUE='14' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime14, "
    sql += "          MAX(CASE WHEN i.ISSUE='14' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney14, "
    sql += "         MAX(CASE WHEN i.ISSUE='14' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney14, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='15' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime15, "
    sql += "          MAX(CASE WHEN i.ISSUE='15' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime15, "
    sql += "          MAX(CASE WHEN i.ISSUE='15' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney15, "
    sql += "         MAX(CASE WHEN i.ISSUE='15' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney15, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='16' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime16, "
    sql += "          MAX(CASE WHEN i.ISSUE='16' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime16, "
    sql += "          MAX(CASE WHEN i.ISSUE='16' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney16, "
    sql += "         MAX(CASE WHEN i.ISSUE='16' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney16, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='17' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime17, "
    sql += "          MAX(CASE WHEN i.ISSUE='17' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime17, "
    sql += "          MAX(CASE WHEN i.ISSUE='17' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney17, "
    sql += "         MAX(CASE WHEN i.ISSUE='17' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney17, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='18' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime18, "
    sql += "          MAX(CASE WHEN i.ISSUE='18' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime18, "
    sql += "          MAX(CASE WHEN i.ISSUE='18' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney18, "
    sql += "         MAX(CASE WHEN i.ISSUE='18' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney18, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='19' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime19, "
    sql += "          MAX(CASE WHEN i.ISSUE='19' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime19, "
    sql += "          MAX(CASE WHEN i.ISSUE='19' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney19, "
    sql += "         MAX(CASE WHEN i.ISSUE='19' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney19, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='20' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime20, "
    sql += "          MAX(CASE WHEN i.ISSUE='20' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime20, "
    sql += "          MAX(CASE WHEN i.ISSUE='20' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney20, "
    sql += "         MAX(CASE WHEN i.ISSUE='20' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney20, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='21' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime21, "
    sql += "          MAX(CASE WHEN i.ISSUE='21' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime21, "
    sql += "          MAX(CASE WHEN i.ISSUE='21' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney21, "
    sql += "         MAX(CASE WHEN i.ISSUE='21' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney21, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='22' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime22, "
    sql += "          MAX(CASE WHEN i.ISSUE='22' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime22, "
    sql += "          MAX(CASE WHEN i.ISSUE='22' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney22, "
    sql += "         MAX(CASE WHEN i.ISSUE='22' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney22, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='23' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime23, "
    sql += "          MAX(CASE WHEN i.ISSUE='23' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime23, "
    sql += "          MAX(CASE WHEN i.ISSUE='23' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney23, "
    sql += "         MAX(CASE WHEN i.ISSUE='23' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney23, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='24' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime24, "
    sql += "          MAX(CASE WHEN i.ISSUE='24' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime24, "
    sql += "          MAX(CASE WHEN i.ISSUE='24' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney24, "
    sql += "         MAX(CASE WHEN i.ISSUE='24' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney24, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='25' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime25, "
    sql += "          MAX(CASE WHEN i.ISSUE='25' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime25, "
    sql += "          MAX(CASE WHEN i.ISSUE='25' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney25, "
    sql += "         MAX(CASE WHEN i.ISSUE='25' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney25, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='26' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime26, "
    sql += "          MAX(CASE WHEN i.ISSUE='26' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime26, "
    sql += "          MAX(CASE WHEN i.ISSUE='26' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney26, "
    sql += "         MAX(CASE WHEN i.ISSUE='26' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney26, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='27' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime27, "
    sql += "          MAX(CASE WHEN i.ISSUE='27' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime27, "
    sql += "          MAX(CASE WHEN i.ISSUE='27' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney27, "
    sql += "         MAX(CASE WHEN i.ISSUE='27' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney27, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='28' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime28, "
    sql += "          MAX(CASE WHEN i.ISSUE='28' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime28, "
    sql += "          MAX(CASE WHEN i.ISSUE='28' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney28, "
    sql += "         MAX(CASE WHEN i.ISSUE='28' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney28, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='29' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime29, "
    sql += "          MAX(CASE WHEN i.ISSUE='29' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime29, "
    sql += "          MAX(CASE WHEN i.ISSUE='29' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney29, "
    sql += "         MAX(CASE WHEN i.ISSUE='29' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney29, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='30' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime30, "
    sql += "          MAX(CASE WHEN i.ISSUE='30' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime30, "
    sql += "          MAX(CASE WHEN i.ISSUE='30' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney30, "
    sql += "         MAX(CASE WHEN i.ISSUE='30' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney30, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='31' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime31, "
    sql += "          MAX(CASE WHEN i.ISSUE='31' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime31, "
    sql += "          MAX(CASE WHEN i.ISSUE='31' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney31, "
    sql += "         MAX(CASE WHEN i.ISSUE='31' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney31, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='32' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime32, "
    sql += "          MAX(CASE WHEN i.ISSUE='32' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime32, "
    sql += "          MAX(CASE WHEN i.ISSUE='32' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney32, "
    sql += "         MAX(CASE WHEN i.ISSUE='32' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney32, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='33' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime33, "
    sql += "          MAX(CASE WHEN i.ISSUE='33' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime33, "
    sql += "          MAX(CASE WHEN i.ISSUE='33' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney33, "
    sql += "         MAX(CASE WHEN i.ISSUE='33' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney33, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='34' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime34, "
    sql += "          MAX(CASE WHEN i.ISSUE='34' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime34, "
    sql += "          MAX(CASE WHEN i.ISSUE='34' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney34, "
    sql += "         MAX(CASE WHEN i.ISSUE='34' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney34, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='35' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime35, "
    sql += "          MAX(CASE WHEN i.ISSUE='35' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime35, "
    sql += "          MAX(CASE WHEN i.ISSUE='35' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney35, "
    sql += "         MAX(CASE WHEN i.ISSUE='35' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney35, "
    sql += " "
    sql += "          MAX(CASE WHEN i.ISSUE='36' and i.ARAP_STATE ='1'  THEN FROM_UNIXTIME(i.`PAY_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) payTime36, "
    sql += "          MAX(CASE WHEN i.ISSUE='36' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') ELSE NULL END ) invoiceTime36, "
    sql += "          MAX(CASE WHEN i.ISSUE='36' and i.`INVOICE_MONEY` is not null and FROM_UNIXTIME(i.`INVOICE_TIME`/1000 ,'%%Y/%%m/%%d') is not null THEN i.`INVOICE_MONEY` ELSE NULL END ) invoiceMoney36, "
    sql += "         MAX(CASE WHEN i.ISSUE='36' and i.ARAP_STATE ='1' and i.AMOUNT_MONEY is not null   THEN i.`AMOUNT_MONEY` ELSE NULL END ) receiveMoney36, "
    sql += "           "
    sql += "          SUM(i.INVOICE_MONEY) AS invoiceMoneyTotal, "
    sql += "          date_format(now(), '%%m') as invoiceMonthsYear, "
    sql += "          sum(case when i.`INVOICE_TIME` >= unix_timestamp(date_format(now(), '%%Y-01-01'))*1000 then 1 else 0 end) as invoicePeriodsYear "
    sql += "          FROM ICE_DEVICE_RENT_INFO_HISTORY AS h INNER JOIN ICE_arap AS i "
    sql += "          ON h.`body_num`=i.body_num AND h.`CONTRACT_ID`=i.`CONTRACT_ID` "
    # sql += "          AND i.`RECEIVABLE_ID`='5F8CDA30B99C4841BB236695B21C5E2F' "
    sql += "            "
    if startTime:
        sql += "               and h.RENT_TIME >= " + str(startTime) + "  "
    if endTime:
        sql += "               and h.RENT_TIME <  " + str(endTime) + " "
    sql += "                "
    sql += "            "
    sql += "            "
    sql += "           "
    # 如果存在参数添加条件
    if deviceModel:
        sql += "        and    h.`DEVICE_MODEL` ='" + deviceModel + "'               "
    if company:
        sql += "        and    h.`AGENT_CO` ='" + company + "'               "
    if keyword:
        sql += "            and  (h.BODY_NUM like '%%" + keyword + "%%' or h.CONTRACT_NO like '%%" + keyword + "%%')"
    sql += "            "
    sql += "          GROUP BY h.contract_id,h.BODY_NUM "
    sql += "          order BY h.`RENT_TIME`"
    rentCostData = pd.read_sql_query(sql, ice_conn)

    # 列顺序
    order_col = [
        # 没有 ,
        'signingMonth',
        'agentCo',
        # ,
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
        'invoiceTime2',
        'payTime2',
        'receiveMoney2',
        'invoiceTime3',
        'payTime3',
        'receiveMoney3',
        'invoiceTime4',
        'payTime4',
        'receiveMoney4',
        'invoiceTime5',
        'payTime5',
        'receiveMoney5',
        'invoiceTime6',
        'payTime6',
        'receiveMoney6',
        'invoiceTime7',
        'payTime7',
        'receiveMoney7',
        'invoiceTime8',
        'payTime8',
        'receiveMoney8',
        'invoiceTime9',
        'payTime9',
        'receiveMoney9',
        'invoiceTime10',
        'payTime10',
        'receiveMoney10',
        'invoiceTime11',
        'payTime11',
        'receiveMoney11',
        'invoiceTime12',
        'payTime12',
        'receiveMoney12',
        'invoiceTime13',
        'payTime13',
        'receiveMoney13',
        'invoiceTime14',
        'payTime14',
        'receiveMoney14',
        'invoiceTime15',
        'payTime15',
        'receiveMoney15',
        'invoiceTime16',
        'payTime16',
        'receiveMoney16',
        'invoiceTime17',
        'payTime17',
        'receiveMoney17',
        'invoiceTime18',
        'payTime18',
        'receiveMoney18',
        'invoiceTime19',
        'payTime19',
        'receiveMoney19',
        'invoiceTime20',
        'payTime20',
        'receiveMoney20',
        'invoiceTime21',
        'payTime21',
        'receiveMoney21',
        'invoiceTime22',
        'payTime22',
        'receiveMoney22',
        'invoiceTime23',
        'payTime23',
        'receiveMoney23',
        'invoiceTime24',
        'payTime24',
        'receiveMoney24',
        'invoiceTime25',
        'payTime25',
        'receiveMoney25',
        'invoiceTime26',
        'payTime26',
        'receiveMoney26',
        'invoiceTime27',
        'payTime27',
        'receiveMoney27',
        'invoiceTime28',
        'payTime28',
        'receiveMoney28',
        'invoiceTime29',
        'payTime29',
        'receiveMoney29',
        'invoiceTime30',
        'payTime30',
        'receiveMoney30',
        'invoiceTime31',
        'payTime31',
        'receiveMoney31',
        'invoiceTime32',
        'payTime32',
        'receiveMoney32',
        'invoiceTime33',
        'payTime33',
        'receiveMoney33',
        'invoiceTime34',
        'payTime34',
        'receiveMoney34',
        'invoiceTime35',
        'payTime35',
        'receiveMoney35',
        'invoiceTime36',
        'payTime36',
        'receiveMoney36',
        'invoiceMoneyTotal',
        'invoiceFinalDate',
        'amountFinalDate',
        # ,
        'YearTicketMonth',
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

    # 替换字段表
    rename_colums = {
        # 没有 :'开票月份',
        'signingMonth': '合同签订月份',
        'agentCo': '公司名',
        #:'1.0系统合同号',
        'contractNo': '2.0系统合同号',
        'customContractNo': '手工合同号',
        'deviceModel': '型号',
        'period': '期间',
        'account': '数量',
        #:'1.0机身号',
        'bodyNum': '2.0机身号',
        'deviceLeaseMoney': '合同总金额',
        # :'代理商额外收取客户维护费',
        'invoiceTime1': '开票日期T1',
        'payTime1': '付款日期T1',
        'receiveMoney1': '首期',
        'invoiceTime2': '开票日期T2',
        'payTime2': '付款日期T2',
        'receiveMoney2': '2期',
        'invoiceTime3': '开票日期T3',
        'payTime3': '付款日期T3',
        'receiveMoney3': '3期',
        'invoiceTime4': '开票日期T4',
        'payTime4': '付款日期T4',
        'receiveMoney4': '4期',
        'invoiceTime5': '开票日期T5',
        'payTime5': '付款日期T5',
        'receiveMoney5': '5期',
        'invoiceTime6': '开票日期T6',
        'payTime6': '付款日期T6',
        'receiveMoney6': '6期',
        'invoiceTime7': '开票日期T7',
        'payTime7': '付款日期T7',
        'receiveMoney7': '7期',
        'invoiceTime8': '开票日期T8',
        'payTime8': '付款日期T8',
        'receiveMoney8': '8期',
        'invoiceTime9': '开票日期T9',
        'payTime9': '付款日期T9',
        'receiveMoney9': '9期',
        'invoiceTime10': '开票日期T10',
        'payTime10': '付款日期T10',
        'receiveMoney10': '10期',
        'invoiceTime11': '开票日期T11',
        'payTime11': '付款日期T11',
        'receiveMoney11': '11期',
        'invoiceTime12': '开票日期T12',
        'payTime12': '付款日期T12',
        'receiveMoney12': '12期',
        'invoiceTime13': '开票日期T13',
        'payTime13': '付款日期T13',
        'receiveMoney13': '13期',
        'invoiceTime14': '开票日期T14',
        'payTime14': '付款日期T14',
        'receiveMoney14': '14期',
        'invoiceTime15': '开票日期T15',
        'payTime15': '付款日期T15',
        'receiveMoney15': '15期',
        'invoiceTime16': '开票日期T16',
        'payTime16': '付款日期T16',
        'receiveMoney16': '16期',
        'invoiceTime17': '开票日期T17',
        'payTime17': '付款日期T17',
        'receiveMoney17': '17期',
        'invoiceTime18': '开票日期T18',
        'payTime18': '付款日期T18',
        'receiveMoney18': '18期',
        'invoiceTime19': '开票日期T19',
        'payTime19': '付款日期T19',
        'receiveMoney19': '19期',
        'invoiceTime20': '开票日期T20',
        'payTime20': '付款日期T20',
        'receiveMoney20': '20期',
        'invoiceTime21': '开票日期T21',
        'payTime21': '付款日期T21',
        'receiveMoney21': '21期',
        'invoiceTime22': '开票日期T22',
        'payTime22': '付款日期T22',
        'receiveMoney22': '22期',
        'invoiceTime23': '开票日期T23',
        'payTime23': '付款日期T23',
        'receiveMoney23': '23期',
        'invoiceTime24': '开票日期T24',
        'payTime24': '付款日期T24',
        'receiveMoney24': '24期',
        'invoiceTime25': '开票日期T25',
        'payTime25': '付款日期T25',
        'receiveMoney25': '25期',
        'invoiceTime26': '开票日期T26',
        'payTime26': '付款日期T26',
        'receiveMoney26': '26期',
        'invoiceTime27': '开票日期T27',
        'payTime27': '付款日期T27',
        'receiveMoney27': '27期',
        'invoiceTime28': '开票日期T28',
        'payTime28': '付款日期T28',
        'receiveMoney28': '28期',
        'invoiceTime29': '开票日期T29',
        'payTime29': '付款日期T29',
        'receiveMoney29': '29期',
        'invoiceTime30': '开票日期T30',
        'payTime30': '付款日期T30',
        'receiveMoney30': '30期',
        'invoiceTime31': '开票日期T31',
        'payTime31': '付款日期T31',
        'receiveMoney31': '31期',
        'invoiceTime32': '开票日期T32',
        'payTime32': '付款日期T32',
        'receiveMoney32': '32期',
        'invoiceTime33': '开票日期T33',
        'payTime33': '付款日期T33',
        'receiveMoney33': '33期',
        'invoiceTime34': '开票日期T34',
        'payTime34': '付款日期T34',
        'receiveMoney34': '34期',
        'invoiceTime35': '开票日期T35',
        'payTime35': '付款日期T35',
        'receiveMoney35': '35期',
        'invoiceTime36': '开票日期T36',
        'payTime36': '付款日期T36',
        'receiveMoney36': '36期',
        'invoiceMoneyTotal': '开票合计',
        'invoiceFinalDate': '开票日期（期未）',
        'amountFinalDate': '付款日期（期未）',
        #:'收期未购机款',
        'YearTicketMonth': '当年开票月数',
        'YearTicketNumber': '当年开票期数',
        'itvacost': ' 租机成本 ',
        'itvacostAvg': ' 租机成本（月成本） ',
        #:' 材料成本 ',
        #:' 材料成本（月成本） ',
        'yearActualCost': _d_now_date.strftime("%Y")+'年实际成本  ',
        #:'  2018年实际材料成本  ',
        'yearActualCostBuckle': _d_now_date.strftime("%Y")+'年实际成本（扣10%）  ',
        #:'  2018年实际材料成本（扣10%）  '
    }

    if len(rentCostData)>0:
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
            returnData = time.strftime("%Y/%m/%d", time.localtime(int(time.mktime(returnData.timetuple()))))
            return returnData

        # 获取invoiceFinalDate
        def get_amountFinalDate(parajson):
            x, y = parajson
            x = datetime.datetime.fromtimestamp(x / 1000)
            returnData = datetime_offset_by_month(x, y)
            # returnData = int(time.mktime(returnData.timetuple())) * 1000
            returnData = time.strftime("%Y/%m/%d", time.localtime(int(time.mktime(returnData.timetuple()))))
            return returnData

        # rentCostData['invoiceFinalDate'] = rentCostData[['signingDate', 'leaseTerm']].apply(get_invoiceFinalDate, axis=1)
        # rentCostData['amountFinalDate'] = rentCostData[['signingDate', 'leaseTerm']].apply(get_amountFinalDate, axis=1)

        # 获取U8上面的数据原值
        sql = "select AVG(itvacost) as itvacost,cinvcode as deviceModel ,cdefine22 as bodyNum,max(cdefine23) as monum,max(cdefine24) as nsnum  from kctranslist group by cinvcode,cdefine22"
        u8CostData = pd.read_sql_query(sql, conn)
        # 通过机身号和型号记性合并
        rentCostDatareturn = pd.merge(rentCostData, u8CostData, how='left', on=['deviceModel', 'bodyNum'])
        # 获取平均租机成本
        rentCostDatareturn["itvacostAvg"] = rentCostDatareturn["itvacost"].apply(lambda x: float('%.2f' % (x / 36)))

        # 获取当年实际成本
        def get_yearActualCost(parajson):
            x, y = parajson
            return float("%.2f"%(x * y))

        rentCostDatareturn["yearActualCost"] = rentCostDatareturn[['YearTicketMonth', 'itvacostAvg']].apply(
            get_yearActualCost, axis=1)
        # 获取当年实际成本扣10%
        rentCostDatareturn["yearActualCostBuckle"] = rentCostDatareturn["yearActualCost"].apply(lambda x: float('%.2f' % (x * 0.9 )))
        # 转中文列明 日期
        # 日期


        # rentCostDatareturn = rentCostDatareturn.ix[:,order_col]
        rentCostDatareturn = rentCostDatareturn.reindex(columns=order_col)
        # rentCostDatareturn = rentCostDatareturn.loc[order_col]
        # rentCostDatareturn = rentCostDatareturn[ order_col ]


        # 删除不需要的列
        for columns_name in rentCostDatareturn.columns:
            if columns_name not in list(rename_colums.keys()):
                del rentCostDatareturn[columns_name]
        rentCostDatareturn.rename(columns=rename_colums, inplace=True)
        return rentCostDatareturn

    rentCostData = rentCostData.reindex(columns=order_col)
    rentCostData.rename(columns=rename_colums, inplace=True)
    return rentCostData
    # return u8CostData, companyData, deviceModelData, rentCostDatareturn, rentCostData


# 例子
#print("start")
# rentCostData,companyData,deviceModelData,rentCostDatareturn,IcerentCostData=get_rent_cost(1527818590000,1529028190000)
#rentCostData, companyData, deviceModelData, rentCostDatareturn, rentCostData = get_rent_cost(keyword="60107001266")
#print("end")

class IceRentCostHandler(BaseHandler):
    def get(self):
        _d_now_month = _d_now_date.strftime('%Y-%m')
        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        keywords = self.get_argument("keywords", '')

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')
        _start_timestamp = int(time.mktime(_d_startDate.timetuple()))

        _d_endDate = (_d_startDate + relativedelta(months=1))
        _end_timestamp = int(time.mktime(_d_endDate.timetuple()))

        rentCostDatareturn = get_rent_cost(
            startTime= str(_start_timestamp*1000),
            endTime= str(_end_timestamp*1000),
            keyword=keywords #"60107001266"
        )
        rentCostDatareturn.fillna('',inplace=True)

        self.render("ice_rent_cost.html",
                    listData=rentCostDatareturn,
                    _start_date=_start_date,
                    )

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

