
// 3rd-party
var hbase = require('hbase');

// Pick table suffixes based on the type. E.g:bigtable_stream_realtime or bigtable_events
var TABLE_TYPE = {'DATA':'_stream_realtime', 'EVENT':'_events'}

// RowKey separator
var RKS = ":";

/**
 * HBaseClient Class
 * @class HBaseClient
 * @module contentapi
 */
var HBaseClient = function() {
	
	/**
	 * Store the established connection with HBase server
	 * 
	 * @property conn
	 * @type Object
	 * @default {}
	 */
	this.conn = {};
};

HBaseClient.prototype = {

  /**
   * To initialize/establish connection with HBase server
   *
   * @method initialize
   */
  initialize : function() {
	  
	  var hbConn = app.get('config').hbase[_.random(0, (app.get('config').hbase.length - 1))];
	  this.conn = new hbase.Client(hbConn);
	  
	  if (this.conn)
		  app.get('logger').info('☺ :: [HBaseClient][initialize]: Established HBase server connection @ ' + hbConn.host +':'+ hbConn.port +')');
	  else
		  app.get('logger').info('☹ :: [HBaseClient][initialize]: Failed to establish HBase server connection @ ' + hbConn.host +':'+ hbConn.port +')');
  },

  /**
   * To create HBase table
   *
   * @method createTable
   * @param {string} tableName, HBase table name
   * @param {object} schema, Table schema Eg. {ColumnSchema:[{name:"DF"}]}
   * @param {function} cb, Callback(error, success)
   */
  createTable : function(tableName, schema, cb) {
    this.getTable(tableName).create(schema, cb);
  },

  /**
   * To get HBase table object
   *
   * @method getTable
   * @param {string} tableName, HBase table name
   */
  getTable : function(tableName) {
    return this.conn.getTable(tableName);
  },

  /**
   * To get HBase row object
   *
   * @method getRow
   * @param {string} tableName, HBase table name
   * @param {string} rowKey, row key
   */
  getRow : function(tableName, rowKey) {
    return this.conn.getRow(tableName, rowKey);  
  },

  /**
   * Store single timeseries data field
   *
   * @method setData
   * @param {object} options, JSON data to store
   * @param {function} cb, Callback function
   *
   * Eg. options =
   *    {tableName:'iot',rowKey:'sakthi@iot.com', column:'info:id'||['info:id','CF:name'],
   *    data:'283335'||['283335','sakthi']}
   */
  setData : function(tableName, options, cb) {
    var self = this;
    
    if(!options.type){
      var error = new Error("[HBaseClient][setData] options.type required.")
      cb.apply(self, [error, false]);
      return;
    }
     
    tableName += TABLE_TYPE[options.type.toUpperCase()];
    delete options.type;
    
    var rowkey = options.rowkey;
    var column = options.column;
    var data = options.data;
    var timestamp = options.timestamp || new Date().getTime();

    self.getRow(tableName, rowkey).put(column, data, timestamp, function(error, success) {
      if (error) {
    	  app.get('logger').error('☹ :: [HBaseClient][setData]: ' +error);
        cb.apply(self, [error, success]);
      } else {
    	  app.get('logger').debug('☺ :: [HBaseClient][setData]: Data for the row '+rowkey+' stored.');
    	  cb.apply(self, [error, success]);
      }
    });
  },

  /**
   * Stores bulk of row/cell data
   *
   * @method setBulkData
   * @param {object} options, JSON data to store
   * @param {function} cb, Callback function
   *
   * Eg.
   *
   * #1. For single row
   * ------------------
   * options =
   *    {tableName:'iot',rowKey:'sakthi@iot.com',
   *    cells :
   *    [ {column:'info:id', timestamp:Date.now(), $:'283335'}
   *    , {column:'name:fname', timestamp:Date.now(), $:'sakthi'}
   *    , {column:'name:lname', timestamp:Date.now(), $:'m'}
   *    ]};
   *
   * (OR)
   *
   * #2. For multiple row
   * --------------------
   *    {tableName:'iot',rowKey:null,
   *    cells :
   *    [ { key:'sakthi@iot.com_283335', column:'info:id', timestamp:Date.now(), $:'283335'}
   *    , { key:'sakthi@iot.com_283335', column:'name:fname', timestamp:Date.now(), $:'sakthi'}
   *    , { key:'sakthi@iot.com_283335', column:'name:lname', timestamp:Date.now(), $:'M'}]
   *    };
   */
  setBulkData : function(tableName, options, cb) {
    var self = this;

    if(!options.type){
      var error = new Error("[HBaseClient][setBulkData] options.type required.")
      cb.apply(self, [error, false]);
      return;
    }
     
    tableName += TABLE_TYPE[options.type.toUpperCase()];
    delete options.type;

    var rowkey = (options.rowkey) ? options.rowkey : null;
    var cells = options.cells;

    self.getRow(tableName, rowkey).put(cells, function(error, success) {
      if (error) {
    	  app.get('logger').error('☹ :: [HBaseClient][setBulkData]: ' +error);
        cb.apply(self, [error, success]);
      } else {
    	  app.get('logger').debug('☺ :: [HBaseClient][setBulkData]: Bulk data stored in table[' + tableName + ']');
        cb.apply(self, [error, success]);
      }
    });
  },

  /**
   * Function to get data for given columns and optionally based on rowKey pattern
   *
   * @method get
   * @param {string} table, table name
   * @param {array} columns, array of columns
   * @param {callback} cb, callback function
   *
   * Eg. options =
   *    {tableName:'iot',rowKey:'sakthi@iot.com'|'sakthi@*'|'*', column:'info:id'||['info:id','CF:name'],
   *    data:'283335'||['283335','sakthi']}
   */
  get : function(tableName, options, cb) {
    var self = this;

    if(!options.type){
      var error = new Error("[HBaseClient][get] options.type required.")
      cb.apply(self, [error, false]);
      return;
    }
     
    tableName += TABLE_TYPE[options.type.toUpperCase()];
    delete options.type;

    var rowkey = (options.rowkey) ? options.rowkey : '*';
    var columns = self._prepareColumns(options.columns, null, options.colFamilyName)

    app.get('logger').info('[HBaseClient][get] columns:' + columns);
    
    self.getRow(tableName, rowkey).get(columns, function(error, cells) {
      if (error) {
    	  app.get('logger').error('☹ :: [HBaseClient][get]: ' +error);
        cb(error, null);
      } else {
    	  app.get('logger').debug('☺ :: [HBaseClient][get]: rowKey:' + rowkey);
        cb(null, self._toJSON(cells, columns, null, options.colFamilyName, true, null));
      }
    });
  },

  /**
   * Fetches HBase table by scaning given columns, optionally between start & end rows as well as
   * filters result based on criteria.
   *
   * @method scan
   * @param {object} options, i.e HBase scanner options such as startRow, endRow, columns, filter,.. etc.
   * @param {callback} cb, callback function
   *
   * Eg. options =
   *    {startRow:stream+":"+startTime,endRow:utils.getEndKey(stream+":"+endTime),column:columns.slice(),filter:filter};
   * where, filter = {"op":"EQUAL","type":"RowFilter","comparator":{"value":".:"+time,"type":"RegexStringComparator"}}
   */
  scan : function(tableName, options, cb) {
    var self = this;

    if(!options.type){
      var error = new Error("[HBaseClient][scan] options.type required.")
      cb.apply(self, [error, false]);
      return;
    }
     
    tableName += TABLE_TYPE[options.type.toUpperCase()];
    delete options.type;

    var columns = self._prepareColumns(options.columns, options.queryOnlyColumns, options.colFamilyName)
    
    var queryOnlyColumns = options.queryOnlyColumns;
    var colFamilyName = options.colFamilyName;

    /* Scanner won't take options.column(s), so need to strip "s" [i.e options.columns -> options.column] */
    options.column = self._prepareColumns(options.columns, options.queryOnlyColumns, options.colFamilyName)
    
    // Below options no need to pass to scanner creation
    delete options.columns
    delete options.queryOnlyColumns;
    delete options.colFamilyName;

    var callback = function(error, cells, scanner, scannerId) {
      var result = { columns : {}, count : 0, data : {} };
      if (error) {
        scanner.delete ();
        cb(error, null);
      } else if (cells) {
        cb(null, self._toJSON(cells, columns, queryOnlyColumns, colFamilyName, true, scannerId));
        app.get('logger').info('[HBaseClient][scan][callback] Retrieve succcess!');
      } else {
        cb(null, result);
        scanner.delete ();
        app.get('logger').info('[HBaseClient][scan][callback] Result exhausted, so scanner '+scannerId+' deleted!');
      }
    }

    var scanner = null;
    
    if(options.scannerId){

      // Use Existing Scanner using "ScannerId"
      app.get('logger').info('[HBaseClient][scan]: Using available scanner ' + options.scannerId);
      scanner = self.getTable(tableName).getScanner(options.scannerId);  
      scanner.get(function(error, cells){
        callback(error, cells, scanner, options.scannerId);
      });

    } else {

      // Create new Scanner using "options"
      app.get('logger').info('[HBaseClient][scan]: Creating new scanner, options:' + JSON.stringify(options));
      scanner = self.getTable(tableName).getScanner();
      scanner.create(options, function(error, scannerId) {
        if (error) {
          app.get('logger').error('[HBaseClient][scan] ' + error);
          cb(error, null);
        } else {
          scanner.get(function(error, cells){
            callback(error, cells, scanner, scannerId);
          });
        }
      });
      
    }
  },
  
  /* ------ Internal function ------ */

  /**
   * Function combines columns used in resultset and columns used only for querying part,
   * also prepends CF name.
   *
   * @method _prepareColumns
   * @param {string} columns, columns included in query & resultset
   * @param {string} queryOnlyColumns, columns only included in query
   * @param {string} colFamilyName, name of the column family
   *
   */
  _prepareColumns : function(columns, queryOnlyColumns, colFamilyName) {
    var tmpColumns = (columns + ( queryOnlyColumns ? "," + queryOnlyColumns : "")).replace(/ /gi, '').split(',');
    var colMap = {};
    for (var i in tmpColumns)
    if (tmpColumns[i].length > 0)
      colMap[colFamilyName + ":" + tmpColumns[i]] = true;
    // This will discard duplicate column, that's Y! using map :-)
    return Object.keys(colMap);
  },

  /**
   * Method converts resultset as JSON
   *
   * @method _toJSON
   * @param {object} cells i.e resultset
   * @param {boolean} splitRowKey to remove stream from rowkey and keep only timestamp
   *
   * @return {object} JSON result
   */
  _toJSON : function(cells, columns, queryOnlyColumns, colFamilyName, splitRowKey, scannerId) {
    var self = this;
    var rows = {}, key, count = 0;
    var inverseColumnMap = self._getColumnMap(columns, true);
    // get inverse column map

    var columsToDiscardMap = {};
    if (queryOnlyColumns !== null) {
      var resultSetColumns = (columns + ( queryOnlyColumns ? "," + queryOnlyColumns : "")).replace(/ /gi, '').split(',');
      queryOnlyColumns = queryOnlyColumns.split(",");
      for (var i in queryOnlyColumns) {
        if (resultSetColumns.indexOf(queryOnlyColumns[i]) == -1)// Don't include columns that are in resultSetColumns
          columsToDiscardMap[colFamilyName + ":" + queryOnlyColumns[i]] = true;
      }
    }
    
    cells && cells.forEach(function(cell) {

      // If splitRowKey enabled, from "streamId:reverse-timestamp:nodeid" only "timestamp:nodeid" will be send to resultset
      key = (splitRowKey && splitRowKey == true) ? (cell.timestamp+RKS+cell.key.split(RKS)[2]) : cell.key;

      if (!rows[key])
        rows[key] = {};

      if (queryOnlyColumns !== null) {
        if (!columsToDiscardMap[cell.column])
          rows[key][inverseColumnMap[cell.column]] = cell['$'];
      } else {
        rows[key][inverseColumnMap[cell.column]] = cell['$'];
      }

    });
    
    return { columns : self._getColumnMap(columns), count : Object.keys(rows).length, data : rows, scannerId : scannerId };
  },

  /**
   * Method converts list of columns as columnmap
   *
   * @method _getColumnMap
   * @param {Array} columns, array of columns names. Default is {"0":"col1","1":"col2","3":"col3"}
   * @param {boolean} isInverse, returns columnmap in inverse order if true. (i.e) {"0":"col1","1":"col2","3":"col3"}
   *
   * @return {object} JSON
   */
  _getColumnMap : function(columns, isInverse) {
    var map = {}, count = 0;
    if (isInverse) {// {"col1":0,"col2":1,"col3":2}
      columns.forEach(function(item) {
        map[item] = count
        count++;
      });
    } else {// {"0":"col1","1":"col2","3":"col3"}
      columns.forEach(function(item) {
        map[count] = item.split(":")[1]
        count++;
      });
    }
    return map;
  }
};

module.exports.HBaseClient = HBaseClient; 
