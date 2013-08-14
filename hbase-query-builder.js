var utils = require('./utils.js')

var COL_FAMILIES = {'EVENT':utils.encode('EF'),'DATA':utils.encode('DF')};

var comparisionOperators = { '<': 'LESS', '<=': 'LESS_OR_EQUAL', '>': 'GREATER', '>=': 'GREATER_OR_EQUAL', '=': 'EQUAL', '!=': 'NOT_EQUAL'};
var logicalCondition = {'OR':'MUST_PASS_ONE', 'AND':'MUST_PASS_ALL'};

var HBaseQueryBuilder = function(data_type){
  this.filter = null;
  this.queryOnlyColumns = {};
  this.ENCODED_CF = COL_FAMILIES[data_type.toUpperCase()]
}

HBaseQueryBuilder.prototype.isLogicalOperator = function(operator){
  return logicalCondition[operator.toUpperCase()] ? true : false;
}

HBaseQueryBuilder.prototype.isLogicalExpression = function(exp){
  var self = this;
  var keys = Object.keys(exp)
  return typeof exp === 'object' && (keys.length == 1) && self.isLogicalOperator(keys[0])
}

HBaseQueryBuilder.prototype.resolveComparisonClause = function(exp){
  var self = this;
  if(exp.column && exp.op && exp['$']){
      if(exp.op.toUpperCase() === 'IN' && Array.isArray(exp['$']))
        return self.resolveInClause(exp); 
      else if(typeof exp['$'] === 'string')
        return {op:comparisionOperators[exp.op],type:"SingleColumnValueFilter",family:self.ENCODED_CF,qualifier:utils.encode(exp.column),comparator:{value:String(exp['$']),type:"BinaryComparator"}}
  } 
  throw new Error('[resolveComparisonClause] Syntax error in query '+JSON.stringify(exp));
}

HBaseQueryBuilder.prototype.resolveInClause = function(exp){
  var self = this;
  if(exp.column && exp.op && Array.isArray(exp['$'])){
    var result = {op:'MUST_PASS_ONE',type:'FilterList',filters:[]};
    var encodedQualifier = utils.encode(exp.column)
    var filter = null;
    exp['$'].forEach(function(value,i){
      filter = {op:comparisionOperators['='],type:"SingleColumnValueFilter",family:self.ENCODED_CF,qualifier:encodedQualifier,comparator:{value:String(value),type:"BinaryComparator"}}
      result.filters.push(filter);
    });
    return result;
  } else {
    throw new Error('[resolveInClause] Syntax error in query '+JSON.stringify(exp));
  }
}

HBaseQueryBuilder.prototype.build = function(query, result){
  var self = this;
  if(self.isLogicalExpression(query)){
    for (key in query){
      if(self.isLogicalOperator(key) && Array.isArray(query[key])){
        result = {op:logicalCondition[key],type:'FilterList',filters:[]};
        query[key].forEach(function(exp,i){
          result.filters.push(self.build(exp));
        });
      } else {
        throw new Error('[resolveInClause] Syntax error in query '+JSON.stringify(query));
      }
    }
  } else {
    return self.resolveComparisonClause(query);
  }
  return result
}


exports.HBaseQueryBuilder = HBaseQueryBuilder;
