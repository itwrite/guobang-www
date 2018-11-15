def filterFields(fields,dict_data):
    if fields == '*':
        return dict_data
    result = {}
    if isinstance(dict_data,dict):
        if isinstance(fields,list):
            for f in fields:
                if isinstance(f,(list,dict)):
                    subDict = filterFields(f,dict_data)
                    for k in subDict:
                        result[k] = subDict[k]
                elif isinstance(f,str) and f in dict_data:
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

def dictMerge(x, y):
    z = x.copy()
    z.update(y)
    return z

def dictToSql(dict_data):
    arr = []
    for key,value in dict_data.items():
        if value.strip()=='':
            arr.append(key)
        else:
            arr.append(key+" AS "+value)
    return ",".join(arr)

def getFieldsSql(fields,dict_data):
    dict_data = filterFields(fields,dict_data)
    return dictToSql(dict_data) 


