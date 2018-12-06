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
import calendar

import pandas as pd
import sqlalchemy
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from tornado.options import define, options

from common import connector
from common.function import *

# u8 db
db_u8 = connector.db('SQL_U8')

# i-synergy db
db_ice = connector.db('SQL_LUBANG')

# guo bang big data
db_local = connector.db('SQL_TEST')

#
# Application
#
define("port", default=8899, help="run on the given port", type=int)

# A thread pool to be used for password hashing with bcrypt.
executor = concurrent.futures.ThreadPoolExecutor(2)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", DepartmentCostHandler),
            (r"/person_cost", PersonCostHandler),
            (r"/accounts_payable", AccountsPayableHandler),
            (r"/temporary_estimation", TemporaryEstimationHandler),
            (r"/capitalized_cost", CapitalizedCostHandler),
            (r"/capitalized_cost_by_class", CapitalizedCostByClassHandler),
            (r"/ice_rent_cost", IceRentCostHandler),
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


class BaseHandler(tornado.web.RequestHandler):

    def data_received(self, chunk):
        pass

    @property
    def db(self):
        return 0

    def get(self, *args, **kwargs):
        return False

    @staticmethod
    def get_except_parent_codes_df(code, data):
        row = data.loc[data.ccode == code]
        for k, n in row.iterrows():
            if 'igrade' in n and n['igrade'] > 1:
                p_len = 2 * n['igrade']
                p_code = code[0:p_len]
                return data.loc[data.ccode != p_code]
        return data

    # 找出当前科目的父科目
    @staticmethod
    def get_parent_code(code, df):
        row = df[df.ccode == code]

        for k, n in row.iterrows():
            if n['igrade'] > 1:
                p_len = 2 * n['igrade']
                p_code = code[0:p_len]
                return df[df.ccode == p_code]
        return row


class DepartmentCostHandler(BaseHandler):
    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 部门编码（传过来的参数）
        _depCode = self.get_argument("cDepCode", 'K')  # 售后服务支持部
        if _depCode == '':
            _depCode = 'K'

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        # 开始时间（传过来的参数）
        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        # 结束时间
        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _d_start_date = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_end_date = datetime.datetime.strptime(_end_date, '%Y-%m')
        _d_end_date = (_d_end_date + relativedelta(months=1))

        sql = " select temp.iYPeriod,temp.ccode,temp.ccode_name,temp.cDepCode,temp.cDepName,sum(temp.total_md) as total_md, sum(temp.total_mc) as total_mc from ( "
        sql += " SELECT DATE_FORMAT(ga.dbill_date,'%%Y-%%m') as iYPeriod,d.cDepName,d.cDepCode,p.cPersonName, ga.cperson_id,c.ccode, c.ccode_name,round(sum(ga.mc),4) as total_mc,round(sum(ga.md),4) as total_md "
        sql += " FROM fin_gl_accvouch ga "
        sql += " JOIN fin_department d on(ga.cdept_id = d.cDepCode) "
        sql += " JOIN fin_code c on ga.ccode = c.ccode and c.bdept=1 "
        sql += " LEFT JOIN fin_person p on p.cPersonCode = ga.cperson_id "
        sql += " WHERE (ga.dbill_date >= '%s' and ga.dbill_date < '%s') and ga.cdept_id='%s' and c.iyear = '%s' "% (
            _d_start_date.strftime('%Y-%m-%d'), _d_end_date.strftime('%Y-%m-%d'),_depCode, _d_start_date.strftime('%Y'))
        sql += " GROUP BY DATE_FORMAT(ga.dbill_date,'%%Y-%%m'),d.cDepName,d.cDepCode,p.cPersonName, ga.cperson_id,c.ccode, c.ccode_name ORDER BY c.ccode_name) temp "
        sql += " group by temp.iYPeriod,temp.ccode,temp.ccode_name,temp.cDepName order by temp.cDepName,temp.ccode; "

        # self.write(sql)
        # self.flush()
        all_vouch_df = pd.read_sql_query(sql, db_local.conn)

        codes_dict = {}
        for i, row in all_vouch_df.iterrows():
            codes_dict[row['ccode_name'] + "___" + row['ccode']] = {"ccode": row['ccode'], "ccode_name": row['ccode_name'], "cDepCode": row['cDepCode'], "cDepName": row['cDepName']}

        # 初始化
        dict_obj = {'科目': [], '科目编码': []}
        columns = ['科目编码', '科目']
        n = 0

        # 获取所有部门
        sql = " select * from fin_department where bDepEnd=1"
        all_department_df = pd.read_sql_query(sql, db_local.conn)

        # 找出某个部门
        department_df = all_department_df[all_department_df['cDepCode'] == _depCode]

        for i, dept in department_df.iterrows():
            dept_name = dept['cDepName']
            for j in range(0, get_month_delta(_d_start_date, _d_end_date)):

                _date = (_d_start_date + relativedelta(months=j))

                _ext_fix = "(" + _date.strftime("%Y-%m") + ")"
                column_name = dept_name + _ext_fix
                if column_name not in dict_obj:
                    dict_obj[column_name] = []
                    columns.append(column_name)

                total_cost = 0
                for k in codes_dict:

                    obj = codes_dict[k]
                    if n == 0:
                        dict_obj['科目'].append(obj['ccode_name'])
                        dict_obj['科目编码'].append(obj['ccode'])
                    cost = self.sum_cost(all_vouch_df, obj['cDepCode'], obj['ccode'], obj['ccode_name'], _date)
                    dict_obj[column_name].append(round(cost, 2))
                    total_cost += cost
                # 合计
                if n == 0:
                    dict_obj['科目'].append("合计")
                    dict_obj['科目编码'].append('')
                dict_obj[column_name].append(round(total_cost, 2))
                n += 1

        drop_index = []
        total2_arr = []
        i = 0
        for c in dict_obj['科目']:
            i_total = 0
            for col in columns:

                if col != '科目' and col != '科目编码':
                    i_total += dict_obj[col][i]
            if i_total == 0:
                drop_index.append(i)

            total2_arr.append(round(i_total, 2))
            i += 1
        #
        columns.append('合计')
        dict_obj['合计'] = total2_arr

        data = pd.DataFrame(dict_obj, columns=columns)
        # data.drop(data.index[drop_index], inplace=True)

        self.render("department_cost.html", listData=data,
                    department_df=department_df,
                    all_department_df=all_department_df,
                    _depCode=_depCode,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )

    @staticmethod
    def sum_cost(df, dep_code, code, code_name, _date):
        new_df = df.query("cDepCode=='%s' and iYPeriod =='%s' and ccode=='%s' and ccode_name=='%s'" % (dep_code, _date.strftime('%Y-%m'), code, code_name))
        y = 0
        for i, r in new_df.iterrows():
            # print(row)
            y += r['total_mc']
        return y


class PersonCostHandler(BaseHandler):

    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _depCode = self.get_argument("cDepCode", '')  # 售后服务支持部
        _personCode = self.get_argument("cPersonCode", '')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _d_start_date = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_end_date = datetime.datetime.strptime(_end_date, '%Y-%m')
        _d_end_date = (_d_end_date + relativedelta(months=1))

        sql = " SELECT ga.ccode,c.ccode_name,ga.cperson_id,ga.cdept_id,ROUND(SUM(ga.mc),4) as total_mc,ROUND(SUM(ga.md),4) as total_md "
        sql += " from fin_gl_accvouch ga "
        sql += " join fin_code c on ga.ccode = c.ccode and c.bperson = 1 and c.iyear = '%s'" % _d_start_date.strftime('%Y')
        sql += " WHERE (ga.dbill_date >= '%s' and ga.dbill_date < '%s')" % (_d_start_date.strftime('%Y-%m'), _d_end_date.strftime('%Y-%m'))
        sql += " GROUP BY ga.ccode,c.ccode_name,ga.cperson_id,ga.cdept_id"

        all_vouch_df = pd.read_sql_query(sql, db_local.conn)

        code_names_dict = {}
        for i, row in all_vouch_df.iterrows():
            code_names_dict[row['ccode']] = row['ccode_name']

        codes = all_vouch_df['ccode'].drop_duplicates()

        # 初始化
        dict_obj = {'科目': [], '科目编码': []}
        columns = ['科目编码', '科目']
        n = 0

        # 获取所有部门
        sql = " select * from fin_department"
        all_department_df = pd.read_sql_query(sql, db_local.conn)

        # 找出某个部门
        # department_df = all_department_df[all_department_df['cDepCode'] == '1']

        for i, dept in all_department_df.iterrows():
            d_name = dept['cDepName']
            # 找出部门对应的所有个人数据
            person_df = self.get_person_df(dept['cDepCode'])
            for j, person in person_df.iterrows():
                p_name = person['cPersonName']
                key = '(' + d_name + ')' + p_name
                if key not in dict_obj:
                    dict_obj[key] = []
                    columns.append(key)

                total_cost = 0

                for code in codes:
                    if n == 0:
                        dict_obj['科目'].append(code_names_dict[code])
                        dict_obj['科目编码'].append(code)
                    vouch_df = all_vouch_df.query("cdept_id == '%s' and cperson_id == '%s'" % (dept['cDepCode'], person['cPersonCode']))
                    cost = self.sum_cost(vouch_df, [code])
                    total_cost += cost
                    dict_obj[key].append(float('%.2f' % cost))
                # 合计
                if n == 0:
                    dict_obj['科目'].append('合计')
                    dict_obj['科目编码'].append('')
                dict_obj[key].append(float('%.2f' % total_cost))
                n += 1

        drop_index = []
        total2_arr = []
        i = 0
        for c in dict_obj['科目']:
            i_total = 0
            for col in columns:

                if col != '科目' and col != '科目编码':
                    i_total += dict_obj[col][i]
            if i_total == 0:
                drop_index.append(i)
            total2_arr.append(round(i_total, 2))

            i += 1
        #
        columns.append('合计')
        dict_obj['合计'] = total2_arr

        data = DataFrame(dict_obj, columns=columns)
        # data.drop(data.index[drop_index], inplace=True)

        self.render("person_cost.html",
                    listData=data,
                    # department_df=department_df,
                    all_department_df=all_department_df,
                    _depCode=_depCode,
                    _start_date=_start_date,
                    _end_date=_end_date,
                    _personCode=_personCode
                    )

    @staticmethod
    def get_person_df(cDepCode, cPersonCode=''):
        sql = "select p.cPersonCode,p.cPersonName,p.cDepCode from fin_person p where 1 "
        if cDepCode != '':
            sql += " and p.cDepCode='%s'" % (cDepCode)
        if cPersonCode != '':
            sql += " and p.cPersonCode='%s'" % (cPersonCode)
        return pd.read_sql_query(sql, db_local.conn)

    @staticmethod
    def sum_cost(df, codes):
        new_df = df.loc[df['ccode'].isin(codes)]
        y = 0
        for i, r in new_df.iterrows():
            y += r['total_mc']
        return y


class AccountsPayableHandler(BaseHandler):
    def get_list(self, result, _start_date, _end_date, main_sql=''):

        self.render("accounts_payable.html",
                    listData=result,
                    _start_date=_start_date,
                    _end_date=_end_date,
                    main_sql=main_sql)

    def get(self):

        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _cCode = self.get_argument("_cCode", '')

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
        _limit_date = _d_endDate

        # 提取出所需要统计的年份，因为可能跨年份统计
        _yearsList = []
        for i in range(0, get_month_delta(_d_startDate, _d_endDate)):
            year = (_d_startDate + relativedelta(months=i)).strftime("%Y")
            if year not in _yearsList:
                _yearsList.append(year)

        # main_sql = fields_sql
        main_sql = "SELECT "
        main_sql += "	ap.iyear as 年,"
        main_sql += "	CONCAT( ap.csign, '-', (case when ap.ino_id<10 then CONCAT('000',ap.ino_id) when ap.ino_id<100 then CONCAT('00',ap.ino_id) when ap.ino_id<1000 then CONCAT('0',ap.ino_id) else ap.ino_id end) ) AS 凭证号,"
        main_sql += "	ap.cVenCode AS 供应商编码,"
        main_sql += "	ap.cVenName AS 供应商名称,"
        main_sql += "	ap.ccode_name AS 科目名称,"
        main_sql += "	ap.cdigest AS 摘要,"
        main_sql += "	ap.md AS 借方金额,"
        main_sql += "	ap.ccode AS 科目编码,"
        main_sql += "	ap.mc AS 贷方金额,"
        main_sql += "	ap.dbill_date AS 制单日期,"
        main_sql += "	ap.cVenPUOMProtocol as 收付款协议编码,"
        main_sql += "	ap.cVenPUOMProtocolName as 收付款协议"
        main_sql += " FROM fin_accounts_payable ap "

        main_sql += " WHERE (ap.dbill_date >= '%s' and ap.dbill_date <'%s')" % (
            _d_startDate.strftime(_s_date_format), _d_endDate.strftime(_s_date_format))
        main_sql += " ORDER BY ap.dbill_date"
        # self.write(main_sql)
        # self.flush()
        # #从数据库里取数据
        result = pd.read_sql_query(main_sql, db_local.conn)

        _protocols = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '/data/vendor_protocols.csv')
        vendor_protocols = {}
        for row in _protocols.iterrows():
            vendor_protocols[row[1]['供应商']] = row[1]['收付款协议']

            # 取得所有付款协议
            agreement_df = pd.read_sql_query("select * from fin_agreement", db_local.conn)
            agreement_protocols = {}
            for row in agreement_df.iterrows():
                agreement_protocols[row[1]['cCode']] = row[1]['cName']

            def get_protocol_name(inp):
                _protocol_name, _protocol_code, vendor_name = inp

                # 假设所有订单都有收付款协议编码
                if _protocol_code != '':

                    # 先去收付款协议里取
                    if _protocol_code in agreement_protocols:
                        _protocol_name = agreement_protocols[_protocol_code]
                    # 如果在收付款协议里取不到，则去cvs所提供的资料里找
                    elif vendor_name in vendor_protocols:
                        _protocol_name = vendor_protocols[vendor_name]
                    # 再找不到就返回空
                    else:
                        _protocol_name = ''
                elif vendor_name in vendor_protocols:
                    _protocol_name = vendor_protocols[vendor_name]
                return _protocol_name

        result['收付款协议'] = result[['收付款协议', '收付款协议编码', '供应商名称']].apply(get_protocol_name, axis=1)

        #
        pay_list = {
            '预付货款': 0,
            '货到付款': 0,
            '月结30天': 30,
            '月结60天': 60,
            '月结15天': 15,
            '月结45天': 45,
            '现金': 0
        }

        # 当前时间
        _d_now_date = datetime.datetime.now()

        # months ago
        _months_before_date = _limit_date + relativedelta(months=-1)

        def get_pay_date2(inp):
            inv_date, pay_mode = inp

            d_delta = pay_list[pay_mode] if pay_mode in pay_list else 0
            if inv_date != '':
                _d_start_date = datetime.datetime.strptime(inv_date, '%Y-%m-%d') if isinstance(inv_date, str) else inv_date

                _delta = int(d_delta / 30) + 1

                finally_date = _d_start_date + relativedelta(months=_delta)
                if int(_months_before_date.strftime('%Y%m')) >= int(finally_date.strftime('%Y%m')):
                    return _months_before_date.strftime("%Y-%m-15")
                return finally_date.strftime("%Y-%m-15")
                # return "%s-%s" % (_d_start_date.strftime("%Y-%m"),
                #                   calendar.mdays[_d_start_date.month]) if d_delta == 0 else finally_date.strftime(
                #     "%Y-%m-15")

            # _date = (_d_now_date + relativedelta(months=1))
            return (_d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")
            # return "%s-%s" % (_d_now_date.strftime("%Y-%m"), calendar.mdays[_d_now_date.month]) if d_delta == 0 else (
            #             _d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")

        # calculate the payday
        result['预计日期'] = result[['制单日期', '收付款协议']].apply(get_pay_date2, axis=1)

        result['制单日期'] = result['制单日期'].apply(lambda x: x.strftime('%Y-%m-%d'))

        # 可切换为直接列表展示
        is_list = False
        if is_list:
            return self.get_list(result, _start_date, _end_date)

        # 取得供应商编码
        vendor_codes = result['供应商编码'].drop_duplicates()

        # 获取相应的科目
        codes_data = self.get_codes_df(_yearsList)
        # print(main_sql)

        data = {}
        for col in result.columns:
            data[col] = []
        data['余额'] = []
        data['方向'] = []
        data['-'] = []
        for venCode in vendor_codes:
            new_data = result.loc[(result["供应商编码"] == venCode)]

            total_balance = self.get_balance(venCode, _d_startDate.strftime(_s_date_format))
            i = 0
            lend_total = 0
            borrow_total = 0
            _len = len(new_data)
            for index, row in new_data.iterrows():
                # the first row
                if i == 0:
                    # 加一行“期初余额"
                    for col in result.columns:
                        if col in data:
                            if col == '摘要':
                                data[col].append('期初余额')
                            else:
                                data[col].append('')
                    if total_balance > 0:
                        data['方向'].append('贷')
                    elif total_balance == 0:
                        data['方向'].append('平')
                    else:
                        data['方向'].append('借')
                    data['余额'].append(round(abs(total_balance), 2))
                    data['-'].append('')  # first

                # normal row

                lend_total = lend_total + row['贷方金额']
                borrow_total = borrow_total + row['借方金额']
                total_balance = total_balance + row['贷方金额'] - row['借方金额']

                for col in result.columns:
                    if col in data:
                        data[col].append(row[col])
                if total_balance > 0:
                    data['方向'].append('贷')
                elif total_balance == 0:
                    data['方向'].append('平')
                else:
                    data['方向'].append('借')
                data['余额'].append(round(abs(total_balance), 2))
                data['-'].append(self.get_code_name(self.get_parent_code(row['科目编码'], codes_data)) + '-' + row['科目名称'])

                # total row
                if i == _len - 1:
                    for col in result.columns:

                        if col in data:
                            if col == '摘要':
                                data[col].append('小计')
                            elif col == '贷方金额':
                                data[col].append(str(round(lend_total, 2)))
                            elif col == '借方金额':
                                data[col].append(str(round(borrow_total, 2)))
                            else:
                                data[col].append(row[col])
                        data[col].append('')

                    if total_balance > 0:
                        data['余额'].append(round(total_balance, 2))
                        data['方向'].append('贷')
                    elif total_balance == 0:
                        data['余额'].append(round(total_balance, 2))
                        data['方向'].append('平')
                    else:
                        data['余额'].append(round(abs(total_balance), 2))
                        data['方向'].append('借')
                    data['方向'].append('')
                    data['余额'].append('')

                    data['-'].append('')  # end
                    data['-'].append('')  # blank

                i = i + 1

        data = DataFrame(data,
                         columns=['供应商编码', '供应商名称', '凭证号', '制单日期', '摘要', '科目编码', '科目名称', '借方金额', '贷方金额', '方向', '余额',
                                  '-'])

        self.render("accounts_payable.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date,
                    main_sql=main_sql)

    # 根据年份获取所有科目

    @staticmethod
    def get_codes_df(i_year):
        if isinstance(i_year, list):
            i_year = "','".join(i_year)
        sql = "select c.i_id,c.ccode,c.cclass,c.igrade,c.ccode_name,c.bend from fin_code c where iyear in('%s') " % i_year
        return pd.read_sql_query(sql, db_local.conn)

    # 获取初期余额
    @staticmethod
    def get_balance(venCode, limitDate):
        # 贷方金额 为 已收到票据但未付款
        sql = "select distinct ap.cVenName as 供应商名称,ap.mc AS 贷方金额, ap.md as 借方金额 from fin_accounts_payable ap where ap.cVenCode='%s' and ap.ccode_name='人民币' and ap.dbill_date < '%s' " % (venCode, limitDate)
        res = pd.read_sql_query(sql, db_local.conn)
        res = res.groupby(by=['供应商名称']).agg({'借方金额': sum, '贷方金额': sum})
        total_amount = 0
        for row in res.iterrows():
            total_amount = total_amount + (row[1]['贷方金额'] - row[1]['借方金额'])
        return total_amount

    # 提取所有科目的名称
    @staticmethod
    def get_code_name(code_df):
        for k, n in code_df.iterrows():
            return n['ccode_name']


class TemporaryEstimationHandler(BaseHandler):
    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')
        _limit_date = _d_endDate = (_d_startDate + relativedelta(months=1))

        sql = " select  "
        sql += " AutoID"
        #
        sql += " ,cPUOMProtocol  AS 收付款协议编码 "
        #
        sql += " ,cVenPUOMProtocol  AS 供应商收付款协议编码 "

        sql += " ,cOrderCode  AS 订单号 "
        sql += " ,dSDate   AS 结算日期 "
        sql += " ,cWhCode   AS 仓库编码 "
        sql += " ,cWhName   AS 仓库"
        sql += " ,dDate_Str   AS 单据日期 "
        sql += " ,cInVouchCode AS 入库单号 "
        sql += " ,cVenName  AS 供应商 "
        sql += " ,cInvCode  AS 存货编码 "
        sql += " ,cInvName  AS 存货名称 "
        sql += " ,cInvStd   AS 规格型号 "
        sql += " ,cComUnitName    AS 主计量单位 "
        sql += " ,iQuantity  AS 数量 "
        sql += " ,iOriTaxCost AS 原币含税单价 "
        sql += " ,iPrice   AS 原币金额 "
        sql += " ,iOriTaxPrice AS 原币税额 "
        # 委外的需要计算加上税
        sql += " ,iPerTaxRate  AS 税率 "
        sql += " ,iTaxPrice  AS 本币税额 "
        sql += " ,iPrice  AS 本币无税合计 "
        sql += " ,iSum   AS 本币价税合计 "
        sql += " ,cExch_Name  AS 币种名称 "
        sql += " ,iMaterialFee AS 材料费 "
        sql += " ,iProcessFee AS 加工费单价 "
        sql += " ,iProcessFee as 加工费 "
        sql += " ,cAgrName AS 收付款协议 "
        sql += " from fin_temporary_estimation where `dDate`<'%s'  ORDER BY `dDate` DESC " % (_limit_date.strftime('%Y-%m-01'))

        result = pd.read_sql_query(sql, db_local.conn)
        # print(result.head(5))
        # result.to_excel("/var/www/bigdata/data/test.xlsx")
        #
        result.fillna('', inplace=True)

        # self.write(sql)
        #
        # self.flush()
        #
        _protocols = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '/data/vendor_protocols.csv')
        vendor_protocols = {}
        for row in _protocols.iterrows():
            vendor_protocols[row[1]['供应商']] = row[1]['收付款协议']

        # 取得所有付款协议
        agreement_df = pd.read_sql_query("select * from fin_agreement", db_local.conn)
        agreement_protocols = {}
        for row in agreement_df.iterrows():
            agreement_protocols[row[1]['cCode']] = row[1]['cName']

        def get_protocol_name(inp):
            _protocol_name, _protocol_code, vendor_name = inp

            # 假设所有订单都有收付款协议编码
            if _protocol_code != '':

                # 先去收付款协议里取
                if _protocol_code in agreement_protocols:
                    _protocol_name = agreement_protocols[_protocol_code]
                # 如果在收付款协议里取不到，则去cvs所提供的资料里找
                elif vendor_name in vendor_protocols:
                    _protocol_name = vendor_protocols[vendor_name]
                # 再找不到就返回空
                else:
                    _protocol_name = ''
            elif vendor_name in vendor_protocols:
                _protocol_name = vendor_protocols[vendor_name]
            return _protocol_name

        result['收付款协议'] = result[['收付款协议', '收付款协议编码', '供应商']].apply(get_protocol_name, axis=1)

        pay_list = {
            '预付货款': 0,
            '货到付款': 0,
            '月结30天': 30,
            '月结60天': 60,
            '月结15天': 15,
            '月结45天': 45,
            '现金': 0
        }

        # 当前时间
        _d_now_date = datetime.datetime.now()

        # months ago
        _months_before_date = _limit_date + relativedelta(months=-1)

        # _mon =
        # self.write(_months_before_date.strftime('%Y%m'))
        # self.write("\r\n")

        def get_pay_date2(inp):
            inv_date, pay_mode, order_sn = inp

            d_delta = pay_list[pay_mode] if pay_mode in pay_list else 0
            if inv_date != '':
                _d_start_date = datetime.datetime.strptime(inv_date, '%Y-%m-%d')

                _delta = int(d_delta / 30) + 1

                finally_date = _d_start_date + relativedelta(months=_delta)
                if int(_months_before_date.strftime('%Y%m')) >= int(finally_date.strftime('%Y%m')):
                    return _months_before_date.strftime("%Y-%m-15")
                return finally_date.strftime("%Y-%m-15")
                # return "%s-%s" % (_d_start_date.strftime("%Y-%m"),
                #                   calendar.mdays[_d_start_date.month]) if d_delta == 0 else finally_date.strftime(
                #     "%Y-%m-15")

            # _date = (_d_now_date + relativedelta(months=1))
            return (_d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")
            # return "%s-%s" % (_d_now_date.strftime("%Y-%m"), calendar.mdays[_d_now_date.month]) if d_delta == 0 else (
            #             _d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")

        # calculate the payday
        result['预计日期'] = result[['单据日期', '收付款协议', '订单号']].apply(get_pay_date2, axis=1)

        dates_list = result['预计日期'].drop_duplicates().sort_values()

        # self.write(",".join(result['预计日期'].drop_duplicates()))
        # self.flush()

        # 计算送货时间
        def get_delivery_date(inp):
            inv_date, pay_mode = inp
            return "%s的%s送货" % (pay_mode, datetime.datetime.strptime(inv_date, '%Y-%m-%d').strftime('%m月'))

        result['送货时间'] = result[['单据日期', '收付款协议']].apply(get_delivery_date, axis=1)

        vendors_names = result['供应商'].drop_duplicates()

        pay_after_agreements = ['货到付款', '货到付款(委外)', '到付']
        pay_before_agreements = ['预付货款', '预付货款(委外)', '现金', '现金(委外)']

        data = {}
        cols = ['供应商',
                # '外购', '外委', '无税价合计',
                '含税总价', '月结方式', '预付', '到付']
        for v in cols:
            data[v] = []
        for d in dates_list:
            k = '%s到期' % (d)
            data[k] = []
            cols.append(k)

        for ven_name in vendors_names:

            # 供应商的名字是从结果集里取的，所以这里一定会有数据
            res_df = result[result['供应商'] == ven_name]
            ven_protocol = ''

            # 外购单统计
            po_total = 0
            # 委外单统计
            wo_total = 0
            # 全部统计
            all_total = 0
            # 无税合计
            all_total_no_tax = 0
            # 到付统计
            pay_after_total = 0
            # 现付、预付统计
            pay_before_total = 0
            # 按协议付款统计
            pay_mode_total = 0

            days_total = {}
            days_delivery_desc = {}
            for d in dates_list:
                k = datetime.datetime.strptime(d, '%Y-%m-%d').strftime("%Y-%m")
                days_total[k] = 0
                days_delivery_desc[k] = []
            # ven_protocols_arr = []
            _i = 0
            for row in res_df.iterrows():

                _i += 1
                if _i == 1:
                    ven_protocol = row[1]['收付款协议']

                # 委外的订单
                if str(row[1]['订单号']).find('WO') > -1:
                    tax_rate = int(row[1]['税率']) if row[1]['税率'] != '' else 0
                    temporary_cost = row[1]['加工费'] * (tax_rate + 100) / 100
                    wo_total += temporary_cost
                    all_total_no_tax += row[1]['加工费'] * 1
                else:
                    all_total_no_tax += row[1]['本币价税合计'] if isinstance(row[1]['本币价税合计'], float) else 0
                    temporary_cost = row[1]['本币价税合计'] if isinstance(row[1]['本币价税合计'], float) else 0
                    po_total += temporary_cost
                # 全统计
                all_total += temporary_cost

                day_mode_pay = 0
                if ven_protocol in pay_after_agreements:
                    pay_after_total += temporary_cost
                elif ven_protocol in pay_before_agreements:
                    pay_before_total += temporary_cost
                else:
                    day_mode_pay = temporary_cost
                    pay_mode_total += temporary_cost

                expect_date_str = datetime.datetime.strptime(row[1]['预计日期'], '%Y-%m-%d').strftime('%Y-%m')
                if expect_date_str not in days_total:
                    days_total[expect_date_str] = 0
                days_total[expect_date_str] += day_mode_pay

                if expect_date_str not in days_delivery_desc:
                    days_delivery_desc[expect_date_str] = []
                if row[1]['送货时间'] not in days_delivery_desc[expect_date_str]:
                    days_delivery_desc[expect_date_str].append(row[1]['送货时间'])
                # if row[1]['收付款协议'] != '':
                #     ven_protocols_arr.append(row[1]['收付款协议'])
                #

            # 准备好数据后，装载到dict中
            data['供应商'].append(ven_name)
            # data['外购'].append(round(po_total, 2))
            # data['外委'].append(round(wo_total, 2))
            # data['无税价合计'].append(round(all_total_no_tax, 2))
            data['含税总价'].append(round(all_total, 2))
            # data['付款方式'].append(ven_protocols_arr[0] if len(ven_protocols_arr) > 0 else "到付")
            data['月结方式'].append(ven_protocol)
            data['预付'].append(round(pay_before_total, 2))
            data['到付'].append(round(pay_after_total, 2))

            for k in days_total:
                data["%s-15到期" % k].append(round(days_total[k], 2))

            # for k in days_delivery_desc:
            #     data[k].append("+".join(days_delivery_desc[k]))

        data = DataFrame(data, columns=cols)

        self.render("temporary_estimation.html",
                    listData=data,
                    sql=sql,
                    _start_date=_start_date,
                    vendors_names=vendors_names
                    )

    def get1(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')
        _limit_date = _d_endDate = (_d_startDate + relativedelta(months=1))
        # 从u8上获取数据
        sql = " select  "
        sql += " rds.AutoID"
        #
        sql += " ,(case when ISNULL(po.cVenPUOMProtocol,'')='' then (case when ISNULL(mo.cVenPUOMProtocol,'')='' then ven.cVenPUOMProtocol else pv.cvenpuomprotocol end) else po.cVenPUOMProtocol end)  AS 收付款协议编码 "
        #
        sql += " ,(case when ISNULL(ven.cVenPUOMProtocol,'')='' then ven.cVenPUOMProtocol else pv.cvenpuomprotocol end)  AS 供应商收付款协议编码 "

        sql += " ,rd.cOrderCode  AS 订单号 "
        sql += " ,rds.dSDate   AS 结算日期 "
        sql += " ,wh.cWhCode   AS 仓库编码 "
        sql += " ,wh.cWhName   AS 仓库"
        sql += " ,CONVERT(varchar(100),rd.dDate , 23)   AS 单据日期 "
        sql += " ,rds.cInVouchCode AS 入库单号 "
        sql += " ,ven.cVenName  AS 供应商 "
        sql += " ,rds.cInvCode  AS 存货编码 "
        sql += " ,iv.cInvName  AS 存货名称 "
        sql += " ,iv.cInvStd   AS 规格型号 "
        sql += " ,cu.cComUnitName    AS 主计量单位 "
        sql += " ,rds.iQuantity  AS 数量 "
        sql += " ,rds.iOriTaxCost AS 原币含税单价 "
        sql += " ,rds.iPrice   AS 原币金额 "
        sql += " ,rds.iOriTaxPrice AS 原币税额 "
        # 委外的需要计算加上税
        sql += " ,mod.iPerTaxRate  AS 税率 "
        sql += " ,rds.iTaxPrice  AS 本币税额 "
        sql += " ,rds.iPrice  AS 本币无税合计 "
        sql += " ,rds.iSum   AS 本币价税合计 "
        sql += " ,rd.cExch_Name  AS 币种名称 "
        sql += " ,ISNULL(rds.iMaterialFee,0) AS 材料费 "
        sql += " ,ISNULL(rds.iProcessFee,0) AS 加工费单价 "
        sql += " ,ISNULL(rds.iProcessFee,0) as 加工费 "
        # sql += " , (case when wh.cWhName='委外仓' then isnull(rds.iProcessFee,0) else isnull(rds.iSum,0) end) as 暂估金额 "
        sql += " ,agr.cName AS 收付款协议 "
        sql += " from RdRecords01 rds "
        sql += "  LEFT JOIN RdRecord01 rd ON(rds.ID = rd.ID) "
        sql += "  LEFT JOIN Warehouse wh ON(rd.cWhCode = wh.cWhCode) "
        sql += "  LEFT JOIN Vendor ven ON(ven.cVenCode = rd.cVenCode) "
        sql += "  LEFT JOIN pu_vendorverify pv ON(pv.cvencode = rd.cVenCode) "
        sql += "  LEFT JOIN PO_Pomain po on po.cPOID = rd.cOrderCode "
        sql += "  LEFT JOIN PO_Podetails pod on po.cPOID = pod.cPOID "
        sql += "  LEFT JOIN Inventory  iv on iv.cInvCode=rds.cInvCode "
        sql += "  LEFT JOIN ComputationUnit cu on iv.cComUnitCode = cu.cComunitCode "
        sql += "  LEFT JOIN AA_Agreement agr on agr.cCode = po.cVenPUOMProtocol "
        sql += "  LEFT JOIN OM_MODetails mod on mod.MODetailsID = rds.iOMoDID "
        sql += "  LEFT JOIN OM_MOMain mo on mo.MOID = mod.MOID "
        sql += " WHERE "
        sql += "  wh.cWhName!='资产仓'"
        sql += "  and ISNULL(rds.dSDate,0)=0 and ISNULL(rd.dkeepdate,0)=0"
        sql += " and rd.dDate<'%s'" % _limit_date.strftime('%Y-%m-01')
        sql += " ORDER BY rd.dDate DESC "

        # self.write(sql)
        # self.flush()

        result = pd.read_sql_query(sql, db_u8.conn)
        # result.to_excel("/var/www/bigdata/data/test.xlsx")
        #
        result.fillna('', inplace=True)

        #
        _protocols = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '/data/vendor_protocols.csv')
        vendor_protocols = {}
        for row in _protocols.iterrows():
            vendor_protocols[row[1]['供应商']] = row[1]['收付款协议']

        # 取得所有付款协议
        agreement_df = pd.read_sql_query("select * from AA_Agreement", db_u8.conn)
        agreement_protocols = {}
        for row in agreement_df.iterrows():
            agreement_protocols[row[1]['cCode']] = row[1]['cName']

        def get_protocol_name(inp):
            _protocol_name, _protocol_code, vendor_name = inp

            # 假设所有订单都有收付款协议编码
            if _protocol_code != '':

                # 先去收付款协议里取
                if _protocol_code in agreement_protocols:
                    _protocol_name = agreement_protocols[_protocol_code]
                # 如果在收付款协议里取不到，则去cvs所提供的资料里找
                elif vendor_name in vendor_protocols:
                    _protocol_name = vendor_protocols[vendor_name]
                # 再找不到就返回空
                else:
                    _protocol_name = ''
            elif vendor_name in vendor_protocols:
                _protocol_name = vendor_protocols[vendor_name]
            return _protocol_name

        result['收付款协议'] = result[['收付款协议', '收付款协议编码', '供应商']].apply(get_protocol_name, axis=1)

        pay_list = {
            '预付货款': 0,
            '货到付款': 0,
            '月结30天': 30,
            '月结60天': 60,
            '月结15天': 15,
            '月结45天': 45,
            '现金': 0
        }

        # 当前时间
        _d_now_date = datetime.datetime.now()

        # months ago
        _months_before_date = _limit_date + relativedelta(months=-1)

        # _mon =
        # self.write(_months_before_date.strftime('%Y%m'))
        # self.write("\r\n")

        def get_pay_date2(inp):
            inv_date, pay_mode, order_sn = inp

            d_delta = pay_list[pay_mode] if pay_mode in pay_list else 0
            if inv_date != '':
                _d_start_date = datetime.datetime.strptime(inv_date, '%Y-%m-%d')

                _delta = int(d_delta / 30) + 1

                finally_date = _d_start_date + relativedelta(months=_delta)
                if int(_months_before_date.strftime('%Y%m')) >= int(finally_date.strftime('%Y%m')):
                    return _months_before_date.strftime("%Y-%m-15")
                return finally_date.strftime("%Y-%m-15")
                # return "%s-%s" % (_d_start_date.strftime("%Y-%m"),
                #                   calendar.mdays[_d_start_date.month]) if d_delta == 0 else finally_date.strftime(
                #     "%Y-%m-15")

            # _date = (_d_now_date + relativedelta(months=1))
            return (_d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")
            # return "%s-%s" % (_d_now_date.strftime("%Y-%m"), calendar.mdays[_d_now_date.month]) if d_delta == 0 else (
            #             _d_now_date + relativedelta(months=1)).strftime("%Y-%m-15")

        # calculate the payday
        result['预计日期'] = result[['单据日期', '收付款协议', '订单号']].apply(get_pay_date2, axis=1)

        dates_list = result['预计日期'].drop_duplicates().sort_values()

        # self.write(",".join(result['预计日期'].drop_duplicates()))
        # self.flush()

        # 计算送货时间
        def get_delivery_date(inp):
            inv_date, pay_mode = inp
            return "%s的%s送货" % (pay_mode, datetime.datetime.strptime(inv_date, '%Y-%m-%d').strftime('%m月'))

        result['送货时间'] = result[['单据日期', '收付款协议']].apply(get_delivery_date, axis=1)

        vendors_names = result['供应商'].drop_duplicates()

        pay_after_agreements = ['货到付款', '货到付款(委外)', '到付']
        pay_before_agreements = ['预付货款', '预付货款(委外)', '现金', '现金(委外)']

        data = {}
        cols = ['供应商',
                # '外购', '外委', '无税价合计',
                '含税总价', '月结方式', '预付', '到付']
        for v in cols:
            data[v] = []
        for d in dates_list:
            k = '%s到期' % (d)
            data[k] = []
            cols.append(k)

        for ven_name in vendors_names:

            # 供应商的名字是从结果集里取的，所以这里一定会有数据
            res_df = result[result['供应商'] == ven_name]
            ven_protocol = ''

            # 外购单统计
            po_total = 0
            # 委外单统计
            wo_total = 0
            # 全部统计
            all_total = 0
            # 无税合计
            all_total_no_tax = 0
            # 到付统计
            pay_after_total = 0
            # 现付、预付统计
            pay_before_total = 0
            # 按协议付款统计
            pay_mode_total = 0

            days_total = {}
            days_delivery_desc = {}
            for d in dates_list:
                k = datetime.datetime.strptime(d, '%Y-%m-%d').strftime("%Y-%m")
                days_total[k] = 0
                days_delivery_desc[k] = []
            # ven_protocols_arr = []
            _i = 0
            for row in res_df.iterrows():

                _i += 1
                if _i == 1:
                    ven_protocol = row[1]['收付款协议']

                # 委外的订单
                if str(row[1]['订单号']).find('WO') > -1:
                    tax_rate = int(row[1]['税率']) if row[1]['税率'] != '' else 0
                    temporary_cost = row[1]['加工费'] * (1 + tax_rate / 100)
                    wo_total += temporary_cost
                    all_total_no_tax += row[1]['加工费'] * 1
                else:
                    all_total_no_tax += row[1]['本币无税合计']
                    temporary_cost = row[1]['本币价税合计']
                    po_total += temporary_cost
                # 全统计
                all_total += temporary_cost

                day_mode_pay = 0
                if ven_protocol in pay_after_agreements:
                    pay_after_total += temporary_cost
                elif ven_protocol in pay_before_agreements:
                    pay_before_total += temporary_cost
                else:
                    day_mode_pay = temporary_cost
                    pay_mode_total += temporary_cost

                expect_date_str = datetime.datetime.strptime(row[1]['预计日期'], '%Y-%m-%d').strftime('%Y-%m')
                if expect_date_str not in days_total:
                    days_total[expect_date_str] = 0
                days_total[expect_date_str] += day_mode_pay

                if expect_date_str not in days_delivery_desc:
                    days_delivery_desc[expect_date_str] = []
                if row[1]['送货时间'] not in days_delivery_desc[expect_date_str]:
                    days_delivery_desc[expect_date_str].append(row[1]['送货时间'])
                # if row[1]['收付款协议'] != '':
                #     ven_protocols_arr.append(row[1]['收付款协议'])
                #

            # 准备好数据后，装载到dict中
            data['供应商'].append(ven_name)
            # data['外购'].append(round(po_total, 2))
            # data['外委'].append(round(wo_total, 2))
            # data['无税价合计'].append(round(all_total_no_tax, 2))
            data['含税总价'].append(round(all_total, 2))
            # data['付款方式'].append(ven_protocols_arr[0] if len(ven_protocols_arr) > 0 else "到付")
            data['月结方式'].append(ven_protocol)
            data['预付'].append(round(pay_before_total, 2))
            data['到付'].append(round(pay_after_total, 2))

            for k in days_total:
                data["%s-15到期" % k].append(round(days_total[k], 2))

            # for k in days_delivery_desc:
            #     data[k].append("+".join(days_delivery_desc[k]))

        data = DataFrame(data, columns=cols)

        self.render("temporary_estimation.html",
                    listData=data,
                    sql=sql,
                    _start_date=_start_date,
                    vendors_names=vendors_names
                    )


class CapitalizedCostByClassHandler(BaseHandler):
    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')

        _months = []
        for j in range(0, get_month_delta(_d_startDate, _d_endDate) + 1):
            _date = (_d_startDate + relativedelta(months=j))
            _months.append(_date.strftime('%Y-%m'))

        sql = "select `日期` as `年份`, `日期` as `期间`,`发退货单号`,`客户编码`,`客户简称`,`币种`,`存货编码`,`存货名称`,`发货数量`,`发货无税金额`,`发货含税金额`,`发货折扣`,`开票数量`,`开票金额`,`未开票数量`,`未开票金额`,`回款金额`,`未回款金额`,`成本单价数量`,`成本单价`,`成本金额`,`发货毛利`,`发货回款毛利`,`日期`,`材料成本单价`,`销售材料成本`,`产品分类`,`销售区域`,`国外国内` from fin_capitalized_cost  where 月份 in ('%s')" % (
            "','".join(_months))
        # 日期 between '%s' and '%s'" %(_d_startDate.strftime("%Y-%m-%d"),_d_endDate.strftime("%Y-%m-%d"))
        # self.write(sql)
        # self.flush()
        # print(sql)
        # #从数据库里取数据
        data = pd.read_sql_query(sql, db_local.conn)
        data['年份'] = data['年份'].apply(lambda x: x.strftime("%Y"))
        data['期间'] = data['期间'].apply(lambda x: x.strftime("%Y%m"))
        data['日期'] = data['日期'].apply(lambda x: x.strftime("%Y-%m-%d"))
        classes = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '/data/CunHuoDangAn-class-20171018.csv')

        def get_class_name(inp):
            _classes = classes[classes['存货编码'] == inp['存货编码']]
            name = '其他'
            if len(_classes) == 1:
                for row in _classes.iterrows():
                    return row[1]['一级类别'] if isinstance(row[1]['二级类别'], float) and math.isnan(row[1]['二级类别']) else \
                        row[1]['二级类别']
            return name

        data['存货分类'] = data[['存货编码']].apply(get_class_name, axis=1)

        # data.drop('AutoID', axis=1, inplace=True)

        # cols = list(data)
        # move the column to head of list using index, pop and insert
        # cols.insert(0, cols.pop(cols.index('存货分类')))
        # use ix to reorder
        # data = data.ix[:, cols]
        # data.groupby(by=['存货分类','存货编码','存货名称']).agg()
        self.render("capitalized_cost.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


class CapitalizedCostHandler(BaseHandler):
    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()

        # 当前月份
        _d_now_date = (_d_now_date + relativedelta(months=-1))
        _d_now_month = _d_now_date.strftime('%Y-%m')

        _start_date = self.get_argument("startDate", _d_now_month)
        if _start_date == '':
            _start_date = _d_now_month

        _end_date = self.get_argument("endDate", _d_now_month)
        if _end_date == '':
            _end_date = _d_now_month

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m')

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m')

        _months = []
        for j in range(0, get_month_delta(_d_startDate, _d_endDate) + 1):
            _date = (_d_startDate + relativedelta(months=j))
            _months.append(_date.strftime('%Y-%m'))

        sql = "select DATE_FORMAT(`日期`,'%Y') as `年份`, DATE_FORMAT(`日期`,'%Y%m') as `期间`,`发退货单号`,`客户编码`,`客户简称`,`币种`,`存货编码`,`存货名称`,`发货数量`,`发货无税金额`,`发货含税金额`,`发货折扣`,`开票数量`,`开票金额`,`未开票数量`,`未开票金额`,`回款金额`,`未回款金额`,`成本单价数量`,`成本单价`,`成本金额`,`发货毛利`,`发货回款毛利`,DATE_FORMAT(`日期`,'%Y-%m-%d') as `日期`,`材料成本单价`,`销售材料成本`,`产品分类`,`销售区域`,`国外国内` from fin_capitalized_cost " + "where 月份 in ('%s')" % (
            "','".join(_months))

        self.write(sql)
        self.flush()
        # 日期 between '%s' and '%s'" %(_d_startDate.strftime("%Y-%m-%d"),_d_endDate.strftime("%Y-%m-%d"))
        # print(sql)
        # #从数据库里取数据
        data = pd.read_sql_query(sql, db_local.conn)
        # data['日期'].apply(lambda x:x.strftime("%Y-%m-%d"))
        classes = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '/data/CunHuoDangAn-class-20171018.csv')

        def get_class_name(inp):
            _classes = classes[classes['存货编码'] == inp['存货编码']]
            name = '其他'
            if len(_classes) == 1:
                for row in _classes.iterrows():
                    return row[1]['一级类别'] if isinstance(row[1]['二级类别'], float) and math.isnan(row[1]['二级类别']) else \
                        row[1]['二级类别']
            return name

        data['存货分类'] = data[['存货编码']].apply(get_class_name, axis=1)
        self.render("capitalized_cost.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date
                    )


class IceRentCostHandler(BaseHandler):
    def get(self):
        # 当前时间
        _d_now_date = datetime.datetime.now()
        _d_now_month = _d_now_date.strftime('%Y-%m-%d')

        _start_date = self.get_argument("startDate", (_d_now_date + relativedelta(months=-2)).strftime('%Y-%m-%d'))
        if _start_date == '':
            _start_date = (_d_now_date + relativedelta(months=-2)).strftime('%Y-%m-%d')

        keywords = self.get_argument("keywords", '')

        _d_startDate = datetime.datetime.strptime(_start_date, '%Y-%m-%d')
        _start_timestamp = int(time.mktime(_d_startDate.timetuple()))

        _end_date = self.get_argument("endDate", (_d_startDate + relativedelta(months=1)).strftime('%Y-%m-%d'))
        if _end_date == '':
            _end_date = _d_now_month

        _d_endDate = datetime.datetime.strptime(_end_date, '%Y-%m-%d')
        _end_timestamp = int(time.mktime(_d_endDate.timetuple()))

        sql = " SELECT "
        sql += " FROM_UNIXTIME(SIGNING_DATE/1000 ,'%%Y/%%m/%%d') as SIGNING_DATE"
        sql += " ,AGENT_CO"
        sql += " ,CUSTOMER"
        sql += " ,SERVICE_CO"
        sql += " ,CONTRACT_ID"
        sql += " ,CONTRACT_NO"
        sql += " ,CUSTOM_CONTRACT_NO"
        sql += " ,DEVICE_MODEL"
        sql += " ,BODY_NUM"
        sql += " ,FROM_UNIXTIME(RENT_TIME/1000,'%%Y/%%m/%%d') as RENT_TIME "
        sql += " ,LEASE_TERM "
        sql += " ,PERIOD_NUM "
        sql += " ,DEVICE_LEASE_MONEY "
        sql += " ,INVOICE_MONEY_TOTAL "

        for i in range(1, 37):
            n = "0" + str(i) if i < 10 else str(i)
            sql += " ,RECEIVE_MONEY" + n
            sql += " ,PAY_TIME" + n
            sql += " ,INVOICE_TIME" + n
            sql += " ,INVOICE_MONEY" + n

        sql += " ,CUR_YEAR_INVOICE_NUM"
        sql += " ,CUR_YEAR_INVOICE_MONTHS"
        sql += " ,END_INVOICE_TIME"
        sql += " ,END_RECEIVE_TIME"
        sql += " ,END_INVOICE_MONEY"
        sql += " ,itvacost"
        sql += " ,itvacost_avg"
        sql += " ,yearActualCost"
        sql += " ,yearActualCostBuckle"

        sql += " from fin_rent_cost where 1 "

        if _start_date != '':
            d = datetime.datetime.strptime(_d_startDate.strftime('%Y-%m-%d 00:00:00'), "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            time_stamp_start = int(time.mktime(t))

            d = datetime.datetime.strptime(_d_endDate.strftime('%Y-%m-%d 23:59:59'), "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            time_stamp_end = int(time.mktime(t))
            sql += " and RENT_TIME >= '%d' and RENT_TIME <= '%d'" % (time_stamp_start * 1000, time_stamp_end * 1000)

        if keywords != '':
            # AGENT_CO like '%" + keywords + "%' or
            sql += " and ( BODY_NUM like '%%" + keywords + "%%' or CONTRACT_NO like '%%" + keywords + "%%' )"

        _sign_date = self.get_argument("signDate", '')
        if _sign_date != '':
            sql += " and FROM_UNIXTIME(SIGNING_DATE/1000 ,'%%Y-%%m-%%d') like '%%" + _sign_date + "%%' "

        # 付款时间查询
        _rent_date = self.get_argument("rentDate", '')
        if _rent_date != '':
            arr = []
            for i in range(1, 37):
                n = "0" + str(i) if i < 10 else str(i)
                arr.append(" FROM_UNIXTIME(PAY_TIME" + n + "/1000,'%%Y-%%m-%%d') like '%%" + _rent_date + "%%' ")
            sql += " and (%s)" % (" or ".join(arr))

        # 付款时间查询
        _invoice_date = self.get_argument("invoiceDate", '')
        if _invoice_date != '':
            arr = []
            for i in range(1, 37):
                n = "0" + str(i) if i < 10 else str(i)
                arr.append(" FROM_UNIXTIME(INVOICE_TIME" + n + "/1000,'%%Y-%%m-%%d') like '%%" + _invoice_date + "%%' ")
            sql += " and (%s)" % (" or ".join(arr))

        sql += " order by RENT_TIME desc"

        # self.write(sql)
        # self.flush()
        data = pd.read_sql_query(sql, db_local.conn)
        data.fillna('', inplace=True)

        # 获取invoiceFinalDate
        def get_date(inp):
            x, y = inp
            if x != '' and x > 0:
                _d = datetime.datetime.fromtimestamp(x / 1000)
                _d = (_d + relativedelta(months=int(y)))
                return _d.strftime('%Y/%m/%d')
            return x

        # data['END_INVOICE_TIME'] = data[['INVOICE_TIME01', 'LEASE_TERM']].apply(get_date, axis=1)
        # data['END_RECEIVE_TIME'] = data[['PAY_TIME01', 'LEASE_TERM']].apply(get_date, axis=1)

        def get_date_by_micotimestamp(x):
            x = 0 if x == '' else x
            timestamp = 0 if x is None else x / 1000
            timestamp = 0 if math.isnan(float(timestamp)) else timestamp
            _d = datetime.datetime.fromtimestamp(timestamp)
            return time.strftime("%Y/%m/%d", time.localtime(int(time.mktime(_d.timetuple())))) if timestamp > 100 else ''

        for i in range(1, 37):
            n = "0" + str(i) if i < 10 else str(i)
            data['PAY_TIME' + n] = data['PAY_TIME' + n].apply(get_date_by_micotimestamp)
            data['INVOICE_TIME' + n] = data['INVOICE_TIME' + n].apply(get_date_by_micotimestamp)

        # 替换字段表
        rename_columns = {
            'CUSTOMER': '租赁方',
            'SIGNING_DATE': '签订日期',
            'AGENT_CO': '公司',
            'CONTRACT_NO': '合同编号',
            'CUSTOM_CONTRACT_NO': '手工合同号',
            'DEVICE_MODEL': '型号',
            'BODY_NUM': '机身号',
            'RENT_TIME': '启租时间',
            'LEASE_TERM': '租期',
            'PERIOD_NUM': '期数',
            'DEVICE_LEASE_MONEY': '合同总金额',
            'INVOICE_MONEY_TOTAL': '开票合计',
            'END_INVOICE_TIME': '期末开票日期',
            'END_RECEIVE_TIME': '期末收款日期',
            'END_INVOICE_MONEY': '期末开票金额'
        }
        sort_columns = ['SIGNING_DATE', 'AGENT_CO', 'CUSTOMER', 'CONTRACT_NO', 'CUSTOM_CONTRACT_NO', 'DEVICE_MODEL', 'BODY_NUM',
                        'RENT_TIME', 'LEASE_TERM', 'PERIOD_NUM', 'DEVICE_LEASE_MONEY', 'INVOICE_MONEY_TOTAL']
        for i in range(1, 37):
            n = "0" + str(i) if i < 10 else str(i)
            rename_columns['RECEIVE_MONEY' + n] = "收款金额" + n
            rename_columns['PAY_TIME' + n] = "收款时间" + n
            rename_columns['INVOICE_MONEY' + n] = "开票金额" + n
            rename_columns['INVOICE_TIME' + n] = "开票时间" + n

            sort_columns.append('RECEIVE_MONEY' + n)
            sort_columns.append('PAID_TIME' + n)
            sort_columns.append('INVOICE_TIME' + n)
            sort_columns.append('INVOICE_MONEY' + n)

        rename_columns['CUR_YEAR_INVOICE_NUM'] = '当年开票期数'
        rename_columns['CUR_YEAR_INVOICE_MONTHS'] = '当年开票月数'

        rename_columns['itvacost'] = '租机成本'
        rename_columns['itvacost_avg'] = '租机成本（月成本）'
        rename_columns['yearActualCost'] = _d_now_date.strftime("%Y") + '年实际成本'
        rename_columns['yearActualCostBuckle'] = _d_now_date.strftime("%Y") + '年实际成本（扣10%）'
        # rename_columns['invoiceFinalDate'] = '开票日期（期未）'
        # rename_columns['amountFinalDate'] = '付款日期（期未）'

        sort_columns.append('END_INVOICE_TIME')
        sort_columns.append('END_RECEIVE_TIME')
        sort_columns.append('END_INVOICE_MONEY')

        sort_columns.append('itvacost')
        sort_columns.append('itvacost_avg')
        sort_columns.append('yearActualCost')
        sort_columns.append('yearActualCostBuckle')

        # 删除不需要的列
        for columns_name in data.columns:
            if columns_name not in list(rename_columns.keys()):
                del data[columns_name]

        data.reindex(columns=sort_columns)

        data.rename(columns=rename_columns, inplace=True)

        self.render("ice_rent_cost.html",
                    listData=data,
                    _start_date=_start_date,
                    _end_date=_end_date,
                    _rent_date=_rent_date,
                    _keywords=keywords
                    )


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
