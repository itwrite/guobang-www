/**
 * Created by zzpzero on 2018/6/14.
 */

(function (_window) {

    var __debug = false,
        __time = (new Date()).getTime(),
        __operators = ['==', '===', '<', '>', '<=', '>=', '<>', '!=', '<<', '>>', 'like'],
        __booleans = ['&&', '||', '|'],
        __booleans_map = {and: '&&', or: '||'},
        __booleans_or_arr = ['||', '|', 'or'];
    var Log = function () {
        if (__debug === true) {
            var time = (new Date()).getTime();
            var arr = ["echo[" + (time - __time) / 1000 + "s]:"];
            for (var i in arguments) {
                if(arguments.hasOwnProperty(i)){
                     arr.push(arguments[i]);
                }
            }
            console.log.apply(this, arr);
        }
    };

    var Helper = {
        /**
         *
         * @param o
         * @returns {string}
         */
        getType: function (o) {
            return Object.prototype.toString.call(o);
        },

        isNumber: function (o) {
            return this.getType(o) === '[object Number]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isArray: function (o) {
            return this.getType(o) === '[object Array]';
        },

        /**
         *
         * @param o
         * @returns {boolean}
         */
        isString: function (o) {
            return this.getType(o) === '[object String]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isDefined: function (o) {
            return typeof o !== 'undefined';
        },

        /**
         *
         * @param elem
         * @param arr
         * @param i
         * @returns {number}
         */
        inArray: function (elem, arr, i) {
            var len;

            if (arr) {
                if ([].indexOf) {
                    return Array.prototype.indexOf.call(arr, elem, i);
                }

                len = arr.length;
                i = i ? i < 0 ? Math.max(0, len + i) : i : 0;

                for (; i < len; i++) {
                    // Skip accessing in sparse arrays
                    if (i in arr && arr[i] === elem) {
                        return i;
                    }
                }
            }
            return -1;
        },

        /**
         *
         * @param obj
         * @returns {boolean}
         */
        isEmptyObject: function (obj) {
            var i = 0, name;
            for (name in obj) {
                i++;
                if (i > 0) return false;
            }
            return true;
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isFunction: function (o) {
            return this.getType(o) === '[object Function]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isObject: function (o) {
            return this.getType(o) === '[object Object]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isBoolean: function (o) {
            return this.getType(o) === '[object Boolean]';
        },

        isNull: function (o) {
            return this.getType(o) === '[object Null]';
        },
        /**
         *
         * @param text
         * @returns {string}
         */
        trim: function (text) {
            return text == null ? "" : ( text + "" ).replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
        },

        /**
         *
         * @param obj
         * @param callback
         * @returns {*}
         */
        each: function (obj, callback) {
            var length, i = 0;
            if (this.isArray(obj)) {
                length = obj.length;
                for (; i < length; i++) {
                    if (callback.call(obj[i], i, obj[i]) === false) {
                        break;
                    }
                }
            } else {
                for (i in obj) {
                    if(obj.hasOwnProperty(i)){
                        if (callback.call(obj[i], i, obj[i]) === false) {
                        break;
                    }
                    }

                }
            }

            return obj;
        },
        copy: function (obj) {
            var that = this;
            if (Helper.isNull(obj) || typeof obj !== 'object') {
                return obj;
            }

            var new_obj = Helper.isArray(obj) ? [] : {};
            Helper.each(obj, function (i, n) {
                new_obj[i] = that.copy(n);
            });
            return new_obj;
        },
        /**
         *
         * @param obj
         * @returns {boolean}
         */
        isWindow: function (obj) {
            /* jshint eqeqeq: false */
            return obj != null && Helper.isDefined(obj['window']) && obj === obj.window;
        },

        /**
         *
         * @param obj
         * @returns {*}
         */
        isPlainObject: function (obj) {
            var key, that = this;

            // Must be an Object.
            // Because of IE, we also have to check the presence of the constructor property.
            // Make sure that DOM nodes and window objects don't pass through, as well
            if (!obj || typeof (obj) !== "object" || obj.nodeType || that.isWindow(obj)) {
                return false;
            }
            var hasOwnProperty = Object.prototype.hasOwnProperty;
            try {

                // Not own constructor property must be Object
                if (obj.constructor && !hasOwnProperty.call(obj, "constructor") && !hasOwnProperty.call(obj.constructor.prototype, "isPrototypeOf")) {
                    return false;
                }
            } catch (e) {

                // IE8,9 Will throw exceptions on certain host objects #9897
                return false;
            }

            // Own properties are enumerated firstly, so to speed up,
            // if last one is own, then all properties are own.
            for (key in obj) {
            }

            return key === undefined || hasOwnProperty.call(obj, key);
        },

        /**
         *
         * @returns {*|{}}
         */
        extend: function () {
            var src, copyIsArray, copy, name, options, clone, that = this,
                target = arguments[0] || {},
                i = 1,
                length = arguments.length,
                deep = false;

            // Handle a deep copy situation
            if (typeof target === "boolean") {
                deep = target;

                // skip the boolean and the target
                target = arguments[i] || {};
                i++;
            }

            // Handle case when target is a string or something (possible in deep copy)
            if (typeof target !== "object" && !Helper.isFunction(target)) {
                target = {};
            }

            // extend itself if only one argument is passed
            if (i === length) {
                target = this;
                i--;
            }

            for (; i < length; i++) {

                // Only deal with non-null/undefined values
                if (( options = arguments[i] ) != null) {

                    // Extend the base object
                    for (name in options) {
                        if(options.hasOwnProperty(name)){
                            src = target[name];
                            copy = options[name];

                            // Prevent never-ending loop
                            if (target === copy) {
                                continue;
                            }

                            // Recurse if we're merging plain objects or arrays
                            if (deep && copy && ( that.isPlainObject(copy) ||
                                ( copyIsArray = that.isArray(copy) ) )) {

                                if (copyIsArray) {
                                    copyIsArray = false;
                                    clone = src && that.isArray(src) ? src : [];

                                } else {
                                    clone = src && that.isPlainObject(src) ? src : {};
                                }

                                // Never move original objects, clone them
                                target[name] = that.extend(deep, clone, copy);

                                // Don't bring in undefined values
                            } else if (copy !== undefined) {
                                target[name] = copy;
                            }
                        }
                    }
                }
            }

            // Return the modified object
            return target;
        }
    };

    /**
     *
     * @param param1
     * @param param2
     * @returns {*}
     */
    function compareFunc(param1, param2) {
        //if both are strings
        if (typeof param1 === "string" && typeof param2 === "string") {
            return param1.localeCompare(param2);
        }
        //if param1 is a number but param2 is a string
        if (typeof param1 === "number" && typeof param2 === "string") {
            return -1;
        }
        //if param1 is a string but param2 is a number
        if (typeof param1 === "string" && typeof param2 === "number") {
            return 1;
        }
        //if both are numbers
        if (typeof param1 === "number" && typeof param2 === "number") {
            if (param1 > param2) return 1;
            if (param1 === param2) return 0;
            if (param1 < param2) return -1;
        }
        return 0;
    }

    /**
     *
     * @param key
     * @param val
     * @param operator
     * @param boolean
     * @constructor
     */
    function ConditionObj(key, operator, val, boolean) {
        var _operator = __operators[0],
            _boolean = __booleans[0];
        Object.defineProperty(this, 'boolean', {
            get: function () {
                return _boolean;
            },
            set: function (boolean) {
                boolean = Helper.inArray(boolean, ['and', 'or'], 0)> -1 ? __booleans_map[boolean] : boolean;
                _boolean = (Helper.isString(boolean) && Helper.inArray(boolean, __booleans, 0) > -1 ? boolean : __booleans[0]);
            }
        });
        Object.defineProperty(this, 'operator', {
            get: function () {
                return _operator;
            },
            set: function (operator) {
                _operator = (Helper.isString(operator) && Helper.inArray(operator, __operators, 0) > -1 ? operator : __operators[0]);
            }
        });
        this.field = key;
        this.value = val;
        this.boolean = boolean;
        this.operator = operator;
    }

    /**
     *
     * @param boolean
     * @constructor
     */
    function WhereObj(boolean) {
        var that = this, _boolean = __booleans[0], _conditionArr = [];
        Object.defineProperty(this, 'boolean', {
            get: function () {
                return _boolean;
            },
            set: function (boolean) {
                boolean = Helper.inArray(boolean, ['and', 'or'], 0) > -1 ? __booleans_map[boolean] : boolean;
                _boolean = (Helper.isString(boolean) && Helper.inArray(boolean, __booleans, 0) > -1 ? boolean : __booleans[0]);
            }
        });
        Object.defineProperty(this, 'conditions', {
            get: function () {
                return _conditionArr;
            }
        });
        this.boolean = boolean;

        /**
         *
         * @param key
         * @param operator
         * @param val
         * @param boolean
         * @returns {WhereObj}
         */
        this.where = function (key, operator, val, boolean) {
            var that = this;
            if (Helper.isObject(key)) {
                boolean = operator;
                for (var k in key) {
                    if(key.hasOwnProperty(k)){
                        that.where(k, __operators[0], key[k], boolean);
                    }
                }
            } else if (Helper.isString(key) && String(key).length > 0) {
                if (arguments.length === 2) {
                    val = operator;
                    _conditionArr.push(new ConditionObj(key, __operators[0], val));
                } else {
                    _conditionArr.push(new ConditionObj(key, operator, val, boolean));
                }
            } else if (Helper.isFunction(key)) {
                boolean = operator;
                var w_obj = new WhereObj(boolean);
                key.call(w_obj);
                _conditionArr.push(w_obj);
            }
            return this;
        };

        // Certain characters need to be escaped so that they can be put into a
        // string literal.
        var escapes = {
                "'": "'",
                '\\': '\\',
                '\r': 'r',
                '\n': 'n',
                '\u2028': 'u2028',
                '\u2029': 'u2029'
            },
            r_trim = /^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g,
            escaper = /\\|'|\r|\n|\u2028|\u2029/g;

        /**
         *
         * @param match
         * @returns {string}
         */
        var _escapeChar = function (match) {
            return '\\' + escapes[match];
        };

        /**
         *
         * @returns {string}
         */
        this.queryString = function () {
            var strArr = [];
            Helper.each(that.conditions, function (i, cond) {
                if (parseInt(i) > 0) {
                    strArr.push(Helper.inArray(cond['boolean'], __booleans_or_arr, 0) > -1 ? ' OR ' : ' AND ');
                }
                if (cond instanceof WhereObj) {
                    strArr.push("(" + cond.queryString() + ")");
                } else if (cond instanceof ConditionObj) {
                    var field = cond['field'];
                    // var value = cond['value'];
                    // value = String(value).replace(escaper, _escapeChar);
                    strArr.push("`" + field + "`");
                    strArr.push(cond['operator']);
                    strArr.push(Helper.isNumber(cond['value']) ? cond['value'] : "'" + (cond['operator'] === 'like' ? String(cond['value']) : String(cond['value'])) + "'");
                }
            });
            return strArr.join(' ');
        };

        var parseCondition = function (params, cond) {
            if (cond instanceof WhereObj) {
                return cond.isMatch(params);
            } else if (cond instanceof ConditionObj) {
                var field = cond['field'];
                var value = cond['value'];
                var j_bok = false;
                if (Helper.isDefined(params[field])) {
                    switch (cond['operator']) {
                        case '=':
                        case '==':
                            j_bok = params[field] === value;
                            break;
                        case '===':
                            j_bok = params[field] === value;
                            break;
                        case '!=':
                            j_bok = params[field] !== value;
                            break;
                        case '<>':
                        case '!==':
                            j_bok = params[field] !== value;
                            break;
                        case '<':
                        case '<<':
                            j_bok = params[field] < value;
                            break;
                        case '<=':
                            j_bok = params[field] <= value;
                            break;
                        case '>':
                        case '>>':
                            j_bok = params[field] > value;
                            break;
                        case '>=':
                            j_bok = params[field] >= value;
                            break;
                        case 'like':
                            var regExpStr = String(value).replace(escaper, _escapeChar).replace(/%/g, '(.*)');
                            var regExpObj = new RegExp("^" + regExpStr + "$");
                            Log(regExpStr);
                            j_bok = regExpObj.test(params[field]);
                            break;
                        default :
                            j_bok = false;
                    }
                }
                return j_bok;
            }
            return Helper.isBoolean(cond) ? cond : false;
        };

        this.isMatch = function (params) {
            var that = this;
            var conditions = that.conditions;
            //如果condition为空，则反回true
            if (conditions.length === 0) {
                return true;
            }
            //var firstCond = conditions[0];
            var bool = false;
            Helper.each(conditions, function (i, cond) {
                if (parseInt(i) > 0) {
                    //if 'or'
                    if (Helper.inArray(cond['boolean'], __booleans_or_arr, 0) > -1) {
                        bool = bool || parseCondition(params, cond);
                    } else {
                        bool = bool && parseCondition(params, cond);
                    }
                } else {
                    bool = parseCondition(params, cond);
                }
            });
            return bool;
        }
    }

    var Vendor = {
        data: [],
        sum: function (field,data) {
            var y = 0, that = this;
            data = Helper.isArray(data)?data:this.data;
            Helper.each(data, function (i, n) {
                if (Helper.isDefined(n[field])) {
                    y = that.add(y, n[field]);
                }
            });
            return y;
        },
        filter: function (field, operator, val, boolean) {
            var that = this, result = [];
            var wObj = new WhereObj('&');
            wObj.where.apply(this, arguments);
            Helper.each(this.data, function (i, row) {
                if (wObj.isMatch(row)) {
                    result.push(row);
                }
            });
            return result;
        },
        count: function (field, operator, val, boolean) {
            var result = this.filter.apply(this, arguments);
            return result.length;
        },
        add: function (arg1, arg2) {

            var r1,r2,m;
            try{r1=arg1.toString().split(".")[1].length}catch(e){r1=0}
            try{r2=arg2.toString().split(".")[1].length}catch(e){r2=0}
            m=Math.pow(10,Math.max(r1,r2));
            return (arg1*m+arg2*m)/m;
        }
    };

    /**
     * 存放所有原始数据
     * @type {Array}
     * @private
     */
    var ____data = [];

    var Algorithm = {
        getColumnObj: function (column) {
            var result = {
                name: "",
                label: null,
                sort: {
                    fx: null,
                    by: 'asc'
                },
                total: {
                    show:false,
                    position: 'top'
                }
            };
            column = Helper.isString(column) ? {name: column} : column;
            if (Helper.isObject(column)) {
                result = Helper.extend(true, result, column);
            }
            return result;
        },
        getGroups: function (list, field, callback) {
            var result = {data: {}, keys: []};
            Helper.each(list, function (i, row) {
                if (Helper.isDefined(row[field])) {
                    var res = true;
                    if (Helper.isFunction(callback)) {
                        res = callback.call(row, key);
                    }
                    if (res !== false) {
                        var key = row[field];
                        if (!Helper.isDefined(result['data'][key])) {
                            result.data[key] = [];
                            result.keys.push(key);
                        }
                        result.data[key].push(row);
                    }
                }
            });
            return result;
        }
    };
    /**
     *
     * @param data {Array}
     * @returns {TableView.fn.init}
     * @constructor
     */
    var TableView = function (data) {
        return new TableView.fn.init(data);
    };

    TableView.fn = TableView.prototype = {
        version: '1.0.1',

        constructor: TableView,
        // Execute a callback for every element in the matched set.
        extend: function () {
            return Helper.extend.apply(this, arguments);
        },
        init: function (data) {
            var that = this;
            ____data = data;
            __time = (new Date()).getTime();
            return this;
        },

        getPivotTableData: function (options) {
            var that = this;
            var defaults = {
                columns: [],
                expressions: []
            };

            var _options = Helper.extend(true, defaults, options);

            var table = {
                header: [{children: [],attr:{}}],
                data: [{
                    children: [],
                    attr:{}
                }]
            };
            Helper.each(_options.columns, function (i, n) {
                n = Algorithm.getColumnObj(n);
                //the header part
                table.header[0].children.push({attr: {}, value: n.label ? n.label : n.name})
            });
            Helper.each(_options.expressions, function (i, n) {
                n = Algorithm.getColumnObj(n);
                //the header part
                table.header[0].children.push({attr: {}, value: n.name})
            });
            /**
             * get the tbody
             * @param data
             * @param columns
             * @returns {Array}
             * @private
             */
            var _getTrsByColumns = function (data, columns) {
                columns = Helper.copy(columns);
                var trs = [], that = this, _td = {attr: {rowspan: 1, colspan: 1}, value: ''};
                if (columns.length > 0) {
                    var col = Algorithm.getColumnObj(columns.pop());

                    var groups = Algorithm.getGroups(data, col.name);


                    //排序
                    if (Helper.isDefined(col['sort']) && !Helper.isNull(col['sort'])) {
                        var sort = col['sort'];
                        /**
                         * sort
                         */
                        var reverse = !1;

                        Log("sort1:", groups.keys);
                        if (Helper.isFunction(sort['fx'])) {

                            reverse = (Helper.isString(sort['by']) && sort['by'].toLowerCase() === 'desc') || !sort['by'] ? 1 : 0;
                            groups.keys = groups.keys.sort(function (a, b) {
                                var A = parseFloat(sort['fx'].call(Vendor, groups.data[a])), B = parseFloat(sort['fx'].call(Vendor, groups.data[b]));
                                return (A < B ? -1 : 1) * [1, -1][+!!reverse];
                            });
                        } else {
                            reverse = (Helper.isString(sort['by']) && sort['by'].toLowerCase() === 'desc') || !sort['by'] ? 1 : 0;
                            groups.keys = groups.keys.sort(function (a, b) {
                                return compareFunc(a, b) * [1, -1][+!!reverse];
                            });
                        }
                    }

                    /**
                     * 暂存 total数据
                     * @type {{}}
                     */
                    var expressionsTotals = {};
                    Helper.each(_options.expressions, function (i, n) {
                        expressionsTotals[n.name] = 0;
                    });

                    /**
                     * 根据分好的组，排好序的keys，遍历生成数据
                     * 每一组生成一行数据，columns如果还有，则递规生成和追加
                     */
                    Helper.each(groups.keys, function (x, k) {
                        var lst = groups.data[k];

                        var tr = {children: [], attr: {}};
                        var td = Helper.copy(_td);
                        td.value = k;
                        td.attr['data-column'] = col.name;
                        tr.children.push(td);
                        trs.push(tr);


                        if (columns.length < 1) {
                            Helper.each(_options.expressions, function (i, n) {
                                var tdx = Helper.copy(_td);
                                if (Helper.isString(n.value)) {
                                    tdx.value = n.value
                                } else if (Helper.isFunction(n.value)) {
                                    Vendor.data = lst;
                                    tdx.value = n.value.call(Helper.extend(Vendor, Helper), lst, ____data);
                                }
                                tdx.value = n.type==='int'?parseInt(tdx.value):parseFloat(tdx.value).toFixed(2);
                                tdx.name = n.name;
                                tr.children.push(tdx);

                                expressionsTotals[n.name] = Vendor.add(expressionsTotals[n.name], tdx.value);
                            });
                        } else {
                            var trs2 = _getTrsByColumns(lst, columns);

                            if (trs2.length > 0) {
                                td.attr.rowspan = trs2.length;
                                Helper.each(trs2, function (i, n) {
                                    if (i === 0) {
                                        tr.attr = Helper.extend(tr.attr, n.attr);
                                        Helper.each(n.children, function (i, d) {
                                            tr.children.push(d);
                                        });
                                    } else {
                                        trs.push(n);
                                    }

                                    //只有不是统计行才统计
                                    if(!Helper.isDefined(n['isTotal'])){
                                        Helper.each(n.children, function (j, m) {
                                            if(Helper.isDefined(m['name'])&& Helper.isDefined(expressionsTotals[m.name])){
                                                expressionsTotals[m.name] = Vendor.add(expressionsTotals[m.name], m.value);
                                            }
                                        });
                                    }
                                });

                            }
                        }
                    });

                    if (col.total.show) {
                        var x_td = Helper.copy(_td);
                        x_td.attr = Helper.extend(true, x_td.attr, {colspan: columns.length + 1, rel: 'total'});
                        x_td.value = '合计:';
                        var x_tr = {children: [x_td], attr: {}, isTotal: col.isTotal};
                        for (var i = 0; i < _options.expressions.length; i++) {
                            var xx_td = Helper.copy(_td);
                            xx_td.value = expressionsTotals[_options.expressions[i].name];
                            xx_td.value = _options.expressions[i].type==='int'?parseInt(xx_td.value):parseFloat(xx_td.value).toFixed(2);
                            xx_td.attr.rel = 'total';
                            x_tr.children.push(xx_td)
                        }

                        if ("bottom" !== col.total.position) {
                            trs.unshift(x_tr);
                        } else {
                            trs.push(x_tr);
                        }

                    }
                }
                return trs;
            };
            var data = _getTrsByColumns(____data, Helper.copy(_options.columns).reverse());
            if (data.length > 0) {
                table.data = data;
            }
            Log(data);

            return table;
        },

        getPivotTableHtml: function (options) {
            var tableData = this.getPivotTableData(options);

            function getAttrHtml(attr) {
                var arr = [];
                Helper.each(attr, function (b, c) {
                    arr.push(b + '="' + c + '"')
                });
                return arr.join(" ");
            }

            var tableHtml = '<table cellpadding="0" cellspacing="0" bordered>';
            tableHtml += '<thead>';
            Helper.each(tableData.header, function (i, n) {
                tableHtml += '<tr ' + getAttrHtml(n.attr) + '>';
                Helper.each(n.children, function (j, m) {
                    tableHtml += '<th ' + getAttrHtml(m.attr) + '>';
                    tableHtml += m.value;
                    tableHtml += '</th>'
                });
                tableHtml += '</tr>'
            });
            tableHtml += '</thead>';

            tableHtml += '<tbody>';
            Helper.each(tableData.data, function (i, n) {
                tableHtml += '<tr ' + getAttrHtml(n.attr) + '>';
                Helper.each(n.children, function (j, m) {
                    tableHtml += '<td ' + getAttrHtml(m.attr) + '>';
                    tableHtml += m.value;
                    tableHtml += '</td>'
                });
                tableHtml += '</tr>'
            });
            tableHtml += '</tbody>';
            tableHtml += '</table>';
            return tableHtml;
        }

    };
    // Give the init function the iModel prototype for later instantiation
    TableView.fn.init.prototype = TableView.fn;

    TableView.fn.size = function () {
        return this.length;
    };
    var

    // Map over Model in case of overwrite
        _TableView = _window.TableView,

    // Map over the M in case of overwrite
        _TV = _window.TV;

    TableView.noConflict = function (deep) {
        if (_window.TV === TableView) {
            _window.TV = _TV;
        }

        if (deep && _window.TableView === TableView) {
            _window.TableView = _TV;
        }

        return TableView;
    };

    _window.TableView = TableView;

    return TableView;


})('undefined' !== typeof window ? window : this);
