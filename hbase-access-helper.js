// 3rd party libraries
var pad = require('pad');

// Application libraries
var utils = require('../lib/utils');
var HBaseQueryBuilder = require('../lib/hbase-query-builder').HBaseQueryBuilder;

var STREAMID_LENGTH = 12;  //Padding size
var MAX_VALUE = 922337203685500;

/**
 * Generates rowkey by combining Fixed length(12 chars) streamId, Reverse timestamp (ie. MAX_VALUE - ts) and NodeId
 * 
 * @method getRowKey
 * @param {string} streamId, Stream Id
 * @param {number} ts, timestamp
 * @param {string} nodeId, Node Id
 */
var getRowKey = function(streamId, ts, nodeId){
  var streamId = pad(STREAMID_LENGTH, String(streamId), 0);
  var ts_reverse = MAX_VALUE - ts;
  return streamId+':'+ts_reverse+':'+((nodeId)?nodeId:'');
}

exports.loadHBaseData = function(tableName, type, streamId, columns, from, to, constraint, limit, scannerId, cb) {
  var CF = {"data":"DF","event":"EF"};
  var colFamilyName = CF[type];
  var options = null;

  var _callback = function(err, data){
    if(err){
      var errMsg = "HBase Error: ";
      if(err.code === 'ECONNREFUSED')
        errMsg += "Unable to reach HBase Server";
      else
        errMsg += err;
      cb(errMsg, null);
    } else {
      cb(null, data);
    }
  }
  
  var streamId = pad(12, String(streamId), 0);
  
  var columnCount = columns.split(',').length;
  var maxRecords = 200;
  var batch = (isNaN(limit) ? maxRecords : (parseInt(limit) <= maxRecords ? parseInt(limit) : maxRecords)) * columnCount;

  // Prepare start & end row values 
  var startRow = (to)?getRowKey(streamId,to):(streamId+'*'); 
  var endRow = (from)?utils.getEndKey(getRowKey(streamId,from)):(utils.getEndKey(streamId)+'*');
  
  var queryOnlyColumns = "";
  
  // Prepare options with filter if exists
  options = {batch:batch,startRow:startRow,endRow:endRow,columns:columns,colFamilyName:colFamilyName,type:type};
  
  if(constraint)
    options.filter = new HBaseQueryBuilder(type).build(constraint)
    
  if(scannerId)
    options.scannerId = scannerId;
    
  // Query only columns(columns won't be part of resultset)
  options.queryOnlyColumns = queryOnlyColumns;
  
  // call scan() of HBaseClient to load history data    
  adapter.hbase.scan(tableName, options, _callback);
}
