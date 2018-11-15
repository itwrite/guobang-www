from dateutil.relativedelta import relativedelta


def get_month_delta(first_date, second_date):
    delta = second_date - first_date
    months = []
    for d in range(1, delta.days):
        _d_date = (first_date + relativedelta(days=d))
        m = _d_date.strftime('%Y-%m')
        if m not in months:
            months.append(m)
    return len(months)


def filter_fields(fields,dict_data):
    if fields == '*':
        return dict_data
    result = {}
    if isinstance(dict_data, dict):
        if isinstance(fields, list):
            for f in fields:
                if isinstance(f,(list,dict)):
                    subDict = filter_fields(f,dict_data)
                    for k in subDict:
                        result[k] = subDict[k]
                elif isinstance(f, str) and f in dict_data:
                    result[f] = dict_data[f]
                else:
                    result[f] = ""
        elif isinstance(fields,dict):
            for f in fields:
                result[f] = fields[f]
        elif isinstance(fields,str):
            if fields in dict_data:
                result[fields] = dict_data[fields]
    return result


def dict_merge(x, y):
    z = x.copy()
    z.update(y)
    return z


def dict_to_sql(dict_data):
    arr = []
    for key,value in dict_data.items():
        if value.strip()=='':
            arr.append(key)
        else:
            arr.append(key+" AS "+value)
    return ",".join(arr)


def get_fields_sql(fields,dict_data):
    dict_data = filter_fields(fields,dict_data)
    return dict_to_sql(dict_data)