//noinspection ThisExpressionReferencesGlobalObjectJS
/**
 * Created by zzpzero on 2018/6/6.
 */

(function (global,factory) {
    return factory(global)
})("undefined" !== typeof window ? window : this, function (_window) {

    var __debug = false,
        __time = (new Date()).getTime();
    var Log = function () {
        if (__debug == true) {
            var time = (new Date()).getTime();
            var arr = ["echo[" + (time - __time) / 1000 + "s]:"];
            for (var i in arguments) {
                arr.push(arguments[i]);
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
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isArray: function (o) {
            return this.getType(o) == '[object Array]';
        },

        /**
         *
         * @param o
         * @returns {boolean}
         */
        isString: function (o) {
            return this.getType(o) == '[object String]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isDefined: function (o) {
            return typeof o != 'undefined';
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
            return this.getType(o) == '[object Function]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isObject: function (o) {
            return this.getType(o) == '[object Object]';
        },
        /**
         *
         * @param o
         * @returns {boolean}
         */
        isBoolean: function (o) {
            return this.getType(o) == '[object Boolean]';
        },

        isNull: function (o) {
            return this.getType(o) == '[object Null]';
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
                    if (callback.call(obj[i], i, obj[i]) === false) {
                        break;
                    }
                }
            }

            return obj;
        },
        deepCopy: function (obj) {
            if (Helper.isNull(obj) || typeof obj != 'object') {
                return obj;
            }
            var new_obj = {};
            for (var attr in obj) {
                new_obj[attr] = this.deepCopy(obj[attr]);
            }
            return new_obj;
        },
        /**
         *
         * @param obj
         * @returns {boolean}
         */
        isWindow: function (obj) {
            /* jshint eqeqeq: false */
            return obj != null && typeof (obj['window'] !='undefined') && obj == obj.window;
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

            // Return the modified object
            return target;
        }
    };

    function Index(valueType) {
        /**
         *
         * @type {string}
         * @private
         */
        this.valueType = valueType;

        /**
         *
         * @type {{}}
         * @private
         */
        this.values = {};

        /**
         *
         * @type {Array}
         */
        this.tables = [];
    }

    /**
     *
     * @param options
     * @constructor
     */
    function DataFrame(options) {
        var that = this;
        /**
         *
         * @type {{onChange: null}}
         */
        var defaults = {
            data:[],
            onChange:null
        };

        /**
         *
         * @type {*|{}}
         */
        this.options = Helper.extend(true,defaults,options);
        /**
         * 默认的表名
         * @type {string}
         * @private
         */
        var ____def_table_name = "_data";

        /**
         * 存放所有原始数据表
         * @type {{}}
         * @private
         */
        var ____db = {};


        /**
         * 初始化默认表
         * @type {Array}
         */
        ____db[____def_table_name] = [];

        /**
         * 存放索引
         * @type {{}}
         * @private
         */
        var ____index_db = {};

        /**
         *
         * @type {Array}
         * @private
         */
        var ____columns = [];

        /**
         * 全局索引
         * @type {number}
         * @private
         */
        var ____i = 0;

        /**
         * 最终筛选结果
         * @type {{}}
         * @private
         */
        var ____view_data = {};

        /**
         *
         * @param table_name = '' {String}
         * @param data {Array}
         * @returns {*}
         */
        this.setTable = function (data, table_name) {

            if (arguments.length == 1) {
                table_name = ____def_table_name;
            } else {
                ____db[table_name] = [];
            }

            if (Helper.isArray(data)) {

                Helper.each(data, function (i, n) {
                    /**
                     * 保存原始数据
                     */
                    ____db[table_name][____i] = n;

                    Helper.each(n, function (k, v) {
                        if (!Helper.isDefined(____index_db[k])) {
                            ____index_db[k] = new Index(Helper.getType(v));
                            if (Helper.inArray(k, ____columns, 0) == -1) {
                                ____columns.push(k);
                            }
                        }

                        if (Helper.inArray(table_name, ____index_db[k].tables, 0) == -1) {
                            ____index_db[k].tables.push(table_name);
                        }

                        if (!Helper.isDefined(____index_db[k].values[v])) {
                            ____index_db[k].values[v] = [];
                        }
                        ____index_db[k].values[v].push(____i);
                    });
                    ____i++;
                });
                Log('____index_db',____index_db);
            }
        };

        /**
         *
         * @returns {Array}
         */
        this.getColumns = function () {
            var result = [];
            Helper.each(____index_db, function (k, n) {
                result.push({name: k, valueType: n.valueType});
            });
            return result;
        };

        /**
         *
         * @param column
         * @param callback
         * @returns {{name: *, values: Array}}
         */
        this.getValues = function (column,callback) {
            var result = {name: column, values: []};
            if (Helper.isDefined(____index_db[column])) {
                if(Helper.isFunction(callback)){
                    Helper.each(____index_db[column].values, function (v, index_list) {
                        if (Helper.getType(1) == ____index_db[column].valueType) {
                            v = parseFloat(v)
                        }
                        if(callback.call(this,v)!=false){
                            result['values'].push(v);
                        }
                    });
                }else{
                    Helper.each(____index_db[column].values, function (v, index_list) {
                        if (Helper.getType(1) == ____index_db[column].valueType) {
                            v = parseFloat(v)
                        }
                        result['values'].push(v);
                    });
                }

            }
            return result;
        };

        this.getValuesDom = function (column,def_text,callback) {
            if(arguments.length==2 && Helper.isFunction(def_text)){
                callback = def_text;
                def_text = ''
            }
            var data = this.getValues(column,callback);
            var select = document.createElement('select');
            select.setAttribute('name',column);
            select.setAttribute('multiple','true');
            var def_op = document.createElement('option');
            def_op.setAttribute('value','');
            def_op.innerHTML = def_text?def_text:'全部';
            select.appendChild(def_op);
            Helper.each(data.values, function (i,n) {
                var option = document.createElement('option');
                option.setAttribute('value', n);
                option.innerHTML = n;
                select.appendChild(option)
            });
            _addEventHandler(select,'change', function () {
                var arr = [];
                for (var i = 0; i < this.length; i++) {
                    if (this.options[i].selected == true){
                        arr.push(this.options[i].value)
                    }
                }
                arr = (arr[0] == '' || arr == '') ? [] : arr;
                that.where(this.getAttribute('name'), arr);
                Log("---------",this.getAttribute('name'), arr);
            });
            return select;
        }

        /**
         *
         * @type {{}}
         * @private
         */
        var ____conditions = {};

        /**
         *
         * @param key
         * @param value
         * @returns {DataFrame}
         */
        this.where = function (key,value) {
            var that = this;
            if (Helper.isObject(key)) {
                for (var k in key) {
                    that.where(k, key[k]);
                }
            } else if (Helper.isString(key) && String(key).length > 0) {
                if(Helper.isDefined(____index_db[key])){
                    var index = ____index_db[key];
                    for(var i in index.tables){
                        var t = index.tables[i];
                        if(!Helper.isDefined(____conditions[t])){
                            ____conditions[t] = {};
                        }
                        ____conditions[t][key] = value;
                    }
                }

            }
            Log(____conditions);
            //return this;
            this.execute();
            return this;
        };

        this.execute = function () {
            ____view_data = _getViewData();
            if(Helper.isFunction(this.options['onChange'])){
                this.options.onChange.apply(this,[____view_data])
            }
            return this;
        };

        //绑定监听事件
        var _addEventHandler = function(target,type,fn){
            if(target.addEventListener){
                target.addEventListener(type,fn);
            }else{
                target.attachEvent("on"+type,fn);
            }
        }

        //移除监听事件
        var _removeEventHandler = function(target,type,fn){
            if(target.removeEventListener){
                target.removeEventListener(type,fn);
            }else{
                target.detachEvent("on"+type,fn);
            }
        }

        /**
         *
         * @param column {String}
         * @param value
         * @returns {Array}
         * @private
         */
        var _indexFilter = function (column,value) {
            var result = [];
            if (Helper.isDefined(____index_db[column])) {
                if(Helper.isArray(value) && value.length==0){
                    Helper.each(____index_db[column].values, function (v, index_lst) {
                        Helper.each(index_lst, function (i, index) {
                            result.push(index);
                        });
                    });
                }else{
                    Helper.each(____index_db[column].values, function (v, index_lst) {
                        if(____index_db[column].valueType==Helper.getType(1)){
                            v = parseFloat(v);
                        }
                        if(Helper.isArray(value)&&Helper.inArray(v,value,0)>-1|| v == value){
                            Helper.each(index_lst, function (i, index) {
                                result.push(index);
                            });
                        }
                    });
                }
            }
            return result;
        };

        /**
         *
         * @param arr1 {Array}
         * @param arr2 {Array}
         * @returns {Array}
         * @private
         */
        var _getInterArr = function (arr1, arr2) {
            var result = [];

            Helper.each(arr1, function (i, n) {
                if(Helper.inArray(n,arr2,0)>-1){
                    result.push(n);
                }
            });
            return result;
        };

        /**
         *
         * @param conditions {{}}
         * @returns {Array}
         * @private
         */
        var _getIndexResult = function (conditions) {
            var index_lst = [],_i=0;
            for(var k in conditions){
                if(_i==0){
                    index_lst = _indexFilter(k,conditions[k])
                }else{
                    index_lst = _getInterArr(index_lst,_indexFilter(k,conditions[k]));
                }
                _i++;
            }
            return index_lst;
        };

        /**
         *
         * @returns {{}}
         * @private
         */
        var _getViewData = function () {
            var that = this;
            var result = {};

            if(!Helper.isEmptyObject(____conditions)){
                var allTables = [];
                Helper.each(____db, function (t_name, d_rows) {
                    allTables.push(t_name);
                    result[t_name] = [];
                });

                Helper.each(allTables, function (i, t) {

                    /**
                     * 如果有，则过滤
                     */
                    if(Helper.isDefined(____conditions[t])){
                        var index_lst = _getIndexResult(____conditions[t]);

                        Helper.each(index_lst, function (j, index) {
                            if (Helper.isDefined(____db[t][index])) {
                                result[t].push(Helper.deepCopy(____db[t][index]));
                            }
                        });
                    }else{
                        result[t] = ____db[t];
                    }
                });
            }else{
                Helper.each(____db, function (t_name, d_rows) {
                    result[t_name] = d_rows;
                });
            }

            return result;
        };



        if(Helper.isArray(this.options.data)){
            this.setTable(this.options.data)
        }else if(Helper.isObject(this.options.data)){
            Helper.each(this.options.data, function (i, n) {
                that.setTable(n,i+"");
            });
        }
        this.execute();
    }

    _window.DataFrame = DataFrame;
});

