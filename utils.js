
// Standard libraries
var fs = require('fs');

// 3rd party libraties
var yaml = require('js-yaml');
var _ = require("underscore");

// Application libraries
var errorTemplate = require('../config/errorTemplate').errorTemplate;

/**
 * Search/filter from list of json object
 *
 * @param {array} data: list of json
 * @param {string} q_by: json field to query
 * @param {string} q: query value
 *
 * @return list of json after filter
 */
exports.search = function(data, q_by, q) {
  if(q){
    var filteredRows = [];
    var regExSearchPattern = new RegExp(q,"i")
    data.forEach(function(item){
      if(regExSearchPattern.test(item[q_by])){
       filteredRows.push(item);
      }
    });
    data = filteredRows;
  }
  return data;
}

/**
 * Converts YAML to JSON data by reading YAML file synchronously
 *
 * @param {string} path: YAML file path
 *
 * @return json
 */
exports.Yaml2JsonSync = function(path) {
  if(!path)
    throw new Error('[utils][Yaml2JsonSync] Provide path to YAML file');
  try{
    var fileContents = fs.readFileSync(path, 'utf-8');
    var json = yaml.load(fileContents);
    return json;
  } catch(e) {
    throw new Error("[utils][Yaml2JsonSync] Error loading YAML data:" + e);
  }
}

/**
 * Converts YAML to JSON data by reading YAML file asynchronously
 *
 * @param {string} path: YAML file path
 * @param {function} callback: Callback function
 *
 * @return json
 */
exports.Yaml2Json = function(path, callback) {

  if(!path)
    throw new Error('Provide path to YAML file');

  fs.readFile(path, 'utf8', function(err, fileContents) {
    if(err) {
      throw new Error('[utils][Yaml2Json] Problem in loading YAML file \"' + config_file + '\". Reason: '+err);
    }
    try {
      var json = yaml.load(fileContents);
    } catch(e) {
      throw new Error("[utils][Yaml2Json] Error loading YAML data:" + e);
    }
    callback(json);
  });
}

/**
 * Encodes string into a 'base64' encoded string
 *
 * @param {string} string: String to be encoded
 *
 * @return string: Base64 encoded string
 */
exports.encode = function(string) {
  return (new Buffer(string, 'utf8')).toString('base64')
}

/**
 * Gives end key of search string
 *
 * @param {string} startKey: Actual search string
 *
 * @return string
 */
exports.getEndKey = function(startKey) {
  if(startKey.length < 3)
    throw new Error('[utils][getEndKey] Search string should have minimum 3 chars. But received ('+ startKey+')');
  return startKey.substring(0,startKey.length-1) + String.fromCharCode(startKey.charCodeAt(startKey.length-1)+1);
}

/**
 * Gives elapsed time in string Eg. 2days
 *
 * @param {object} startTime: Start timestamp
 * @param {object} endTime: End timestamp
 *
 * @return string
 */
exports.elapstedTime = function(startTime,endTime){

  var timeDiff, ms, secs, mins, hrs, days, strElapstedTime;
  
  timeDiff = endTime-startTime;
  
  // calculate milliseconds
  ms = timeDiff % 1000;
  timeDiff = (timeDiff - ms)/1000;
  strElapstedTime = ms+"ms";
  
  // calculate seconds 
  secs = timeDiff % 60;
  timeDiff = (timeDiff - secs)/60;
  strElapstedTime = ((secs>0)?(secs+"sec"+((secs>1)?"s ":" ")+strElapstedTime):strElapstedTime);

  // calculate mins
  mins = timeDiff % 60;
  timeDiff = (timeDiff - mins)/60;
  strElapstedTime = ((mins>0)?(mins+"min"+((mins>1)?"s ":" ")+strElapstedTime):strElapstedTime);

  // calculate hours
  hrs = timeDiff % 24;
  timeDiff = (timeDiff - hrs)/24;
  strElapstedTime = ((hrs>0)?(hrs+"hrs"+((hrs>1)?"s ":" ")+strElapstedTime):strElapstedTime);
  
  // remaining is days
  days = timeDiff;
  strElapstedTime = ((days>0)?(days+"day"+((days>1)?"s ":" ")+strElapstedTime):strElapstedTime);
  
  return strElapstedTime; 
}

/**
 * Calculates "How long ago" from current time from a given date
 *
 * @param {date} date: date value prior to current date
 *
 * @return string
 *
 * Ref: https://github.com/tglines/nodrr/blob/master/global_funcs.js
 */
exports.howLongAgo = function(date) {
  var curr_date = new Date();
  var diff = curr_date - date;
  var sec_diff = Math.floor(diff / 1000);
  var min_diff = Math.floor(diff / 1000 / 60);
  var hrs_diff = Math.floor(diff / 1000 / 60 / 60);
  var days_diff = Math.floor(diff / 1000 / 60 / 60 / 24);

  if(sec_diff == 1)
    return sec_diff + ' second';
  else if(sec_diff < 60)
    return sec_diff + ' seconds';
  else if(min_diff == 1)
    return min_diff + ' minute';
  else if(min_diff < 60)
    return min_diff + ' minutes';
  else if(hrs_diff == 1)
    return hrs_diff + ' hour';
  else if(hrs_diff < 24)
    return hrs_diff + ' hours';
  else if(days_diff == 1)
    return days_diff + ' day';
  else
    return days_diff + ' days';
}


/**
 * Calculates "How long ago" from current time from a given date
 *
 * @param {string} modelname: 
 *
 * @return string
 *
 */
exports.makeKey = function(modelname) {
  var modelKeyNames = {'appspace':'aps'}
  return modelKeyNames[modelname.toLowerCase()];
}

/**
 * Get current time stamp in milliseconds
 *
 * @return number
 *
 */
exports.getCurrentTimeStamp = function() {
  return Math.round(new Date().getTime()/1000.0); 
}

/** 
 * Error handling code
 * 
 * @param {object} res:response object 
 * @param {number} code: internal error code 
 * @param {array} args: arguments for the error message 
 * @param {object} err: error
 *  
 */
exports.sendError = function(res, code, args, err) {
  _formatAndSend('error', res, code, args, err)
}

exports.sendWarn = function(res, code, args, err) {
  _formatAndSend('warn', res, code, args, err)
}

function _formatAndSend(loglevel, res, code, args, err) {
  var httpRespCode = (errorTemplate[code])?errorTemplate[code].code:(_.isNumber(code) && (code>=400 && code<=500))?code:null;
  if(!httpRespCode) throw new Error("[utils][sendError] Invalid HTTP Response code!")
  
  var errMsg = null;
  
  if(errorTemplate[code]) {
    if(args) {
      errMsg = errorTemplate[code].msg.replace(/{(\d+)}/g, function(match, number) {
        return typeof args[number] != 'undefined'?args[number]:match;
      });
    } else {
      errMsg = errorTemplate[code].msg;  
    }
  }
  
  if(err) {
    if(typeof err === 'object') err = JSON.stringify(err);
    errMsg = errMsg?(errMsg+' Detail/Reason:'+err):err;  
  }
  
  if(loglevel === 'error')
    app.get('logger').error('☹ :: ERROR:'+errMsg);
  else if(loglevel === 'warn')
    app.get('logger').warn('☹ :: WARN:'+errMsg);
  
  res.send(httpRespCode, errMsg);
}

/**
 * To merge two json objects
 *
 * @param {object} json1
 * @param {object} json2
 * 
 * @return mergedjson
 *
 */
exports.mergeJSON = function(json1, json2) {
  for(var key in json2) {
    json1[key] = json2[key];
  }
  return json1;
}

/**
 * To check whether key exists in json schema recursively
 *
 * @param {object} schema, model schema
 * @param {string} key, key to check
 * 
 * @return boolean
 *
 */
exports.isKeyExists = function(schema, key2check) {
  var isExists = _checkProperties(schema.properties, key2check);
  return isExists;
}


/*
 * Internal functions
 * ------------------
 */
function _checkProperties(properties, key2check) {
  for (var property in properties) {
    if (property === key2check) {
      return true;
    } else if (properties[property].properties) {
      if (_checkProperties(properties[property].properties, key2check))
        return true;
    }
  }
  return false;
}
