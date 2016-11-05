'use strict';

const _ = require('lodash');
const Item = require('./item');
var diff = require('deep-diff').diff;
const Query = require('./query');
const Scan = require('./scan');
const EventEmitter = require('events').EventEmitter;
const async = require('async');
const utils = require('./utils');
const ParallelScan = require('./parallelScan');
const expressions = require('./expressions');

const internals = {};

const Table = module.exports = function (name, schema, serializer, docClient, logger, optionCreateTable) {
  this.config = { name: name, optionCreateTable: optionCreateTable };
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;
  this.log = logger;

  this._before = new EventEmitter();
  this.before = this._before.on.bind(this._before);

  this._after = new EventEmitter();
  this.after = this._after.on.bind(this._after);
};

Table.prototype.emitItem = function () {
   const self = this;
   return self._after;
}

Table.prototype.initItem = function (attrs) {
  const self = this;

  if (self.itemFactory) {
    return new self.itemFactory(attrs);
  } else {
    return new Item(attrs, self);
  }
};

Table.prototype.tableName = function () {
  if (this.schema.tableName) {
    if (_.isFunction(this.schema.tableName)) {
      return this.schema.tableName.call(this);
    } else {
      return this.schema.tableName;
    }
  } else {
    return this.config.name;
  }
};
Table.prototype.optionCreateTable = function () {
  if (this.schema.optionCreateTable) {
     return this.schema.optionCreateTable;
  } else {
    return null;
  }
};

Table.prototype.sendRequest = function (method, params, callback) {
  const self = this;

  let driver;
  if (_.isFunction(self.docClient[method])) {
    driver = self.docClient;
  } else if (_.isFunction(self.docClient.service[method])) {
    driver = self.docClient.service;
  }

  const startTime = Date.now();
  self.log.info({ params: params }, 'dynogels %s request', method.toUpperCase());
  driver[method].call(driver, params, (err, data) => {
    const elapsed = Date.now() - startTime;

    if (err) {
      self.log.warn({ err: err }, 'dynogels %s error', method.toUpperCase());
      return callback(err);
    } else {
      self.log.info({ data: data }, 'dynogels %s response - %sms', method.toUpperCase(), elapsed);
      return callback(null, data);
    }
  });
};

Table.prototype.get = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  let params = {
    TableName: self.tableName(),
    Key: self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  params = _.merge({}, params, options);

  self.sendRequest('get', params, (err, data) => {
    if (err) {
      return callback(err);
    }

    let item = null;
    if (data.Item) {
      item = self.initItem(self.serializer.deserializeItem(data.Item));
    }

    return callback(null, item);
  });
};

internals.callBeforeHooks = (table, name, startFun, callback) => {
  const listeners = table._before.listeners(name);
  console.error("CALL BEFORE HOOKS");
  console.error(listeners);
  /*try {
   zf();
  } catch (e) {
   console.error(e.stack);
  }*/

  return async.waterfall([startFun].concat(listeners), callback);
};

Table.prototype.create = function (tmpItem, options, callback) {
  const self = this;
  var item = {};
  if( Object.prototype.toString.call( tmpItem ) === '[object Array]' ) {
      var ll = tmpItem.length;
      item = [];
      for(var i = 0; i < ll; i++) {
	var arrItem = tmpItem[i];
        var arItm = {};
        for(var k in arrItem) {
	   if (arrItem.hasOwnProperty(k)) {
		if (!(arrItem[k] === null || arrItem[k] == undefined || arrItem[k] == '')) {
			arItm[k] = arrItem[k];
		}
  	  }
       }       
       item.push(arItm);
     }
  } else {
      for(var k in tmpItem) {
	if (tmpItem.hasOwnProperty(k)) {
		if (!(tmpItem[k] === null || tmpItem[k] == undefined || tmpItem[k] == '')) {
			item[k] = tmpItem[k];
		}
	}
     }
  }
  console.error("create using");
  console.error(item);

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  if (_.isArray(item)) {
    async.map(item, (data, callback) => internals.createItem(self, data, options, callback), callback);
  } else {
    return internals.createItem(self, item, options, callback);
  }
};

internals.createItem = (table, item, options, callback) => {
  const self = table;

  const start = callback => {
    const data = self.schema.applyDefaults(item);

    const paramName = _.isString(self.schema.createdAt) ? self.schema.createdAt : 'createdAt';

    if (self.schema.timestamps && self.schema.createdAt !== false && !_.has(data, paramName)) {
      data[paramName] = new Date().toISOString();
    }

    return callback(null, data);
  };

  internals.callBeforeHooks(self, 'create', start, (err, data) => {
    if (err) {
      return callback(err);
    }

    const result = self.schema.validate(data);

    if (result.error) {
      result.error.message = `${result.error.message} on ${self.tableName()}`;
      return callback(result.error);
    }

    const attrs = utils.omitNulls(data);

    let params = {
      TableName: self.tableName(),
      Item: self.serializer.serializeItem(self.schema, attrs)
    };

    if (options.expected) {
      internals.addConditionExpression(params, options.expected);
      options = _.omit(options, 'expected');
    }

    if (options.overwrite === false) {
      const expected = _.chain([self.schema.hashKey, self.schema.rangeKey]).compact().reduce((result, key) => {
        _.set(result, `${key}.<>`, _.get(params.Item, key));
        return result;
      }, {}).value();

      internals.addConditionExpression(params, expected);
    }

    options = _.omit(options, 'overwrite'); // remove overwrite flag regardless if true or false

    params = _.merge({}, params, options);

    if (self.docClient.service.config.isLocal) {
        internals.simulateAwsDynamoDbEvents(params, self, callback,'put', params, attrs);
    } else {
       self.sendRequest('put', params, err => {
         if (err) {
           return callback(err);
         }
   
         const item = self.initItem(attrs);
         self._after.emit('create', item);
   
         return callback(null, item);
       });
    }


  });
};


internals.simulateAwsDynamoDbEvents = function(params, self, callback, type, dta, attrs) {
        const t = type;
        const paramC = params;
	var query = null;
	if (type == 'update' || type == 'delete') {
	    query = { exec: function(cb) {
	    		  self.query( params.Key[self.schema.hashKey]).exec(cb);
                    }
                };

	} else {
	    query = { exec: function(cb) {
	    		  self.query( dta.Item[self.schema.hashKey] ).exec(cb);
                    }
            };
	}
        query.exec(function (err, g) {
           var origAttribues = {};
	   if (t == 'put' || t == 'update' || t == 'delete') {
	      if (g != undefined) {
		  if (g.Items != undefined) {
		     if (g.Items[0] != undefined) {
	                origAttribues = g.Items[0].attrs;
		     }
		  }
	      }
	   } else {
	      origAttribues = {};
	   }
           //self.sendRequest(type, params, (err, data) => {
           self.sendRequest(t, paramC, (err, data) => {
		if (t == 'put') {
			if (g.Items[0] != undefined) {
			    data = g.Items[0].attrs;
			}
		}
             if (err) {
               return callback(err);
             }
       
             let result = null;
             
	     if (t == 'put' ||  t == 'delete') {
		result = { table: self };
	     } else {
               if (data.Attributes) {
                 result = self.initItem(self.serializer.deserializeItem(data.Attributes));
               }
	     }
       

  	     var hashK= result.table.schema.hashKey;
  	     var rangeK= result.table.schema.rangeKey;
  	     var dataTypes= result.table.schema._modelDatatypes;
	     var r = result.attrs;
             var res = {};
	     var NewImage = {};
   	     var OldImage = undefined;
	     if (t == 'update') {
   	 	   NewImage = internals.printObjectEvent(null, r, dataTypes, null);
   	 	   OldImage = internals.printObjectEvent(null, origAttribues, dataTypes, null);
	     }


		res['ApproximateCreationDateTime']=134323;
	       	res['NewImage']=NewImage;
		if (OldImage != undefined) {
	       	  res['OldImage']=OldImage;
		}
		res['SequenceNumber']=111111;
		res['SizeBytes']=131;
		res['StreamViewType']='NEW_AND_OLD_IMAGES';

	     /*
  	    var dataTypes= result.table.schema._modelDatatypes;
	    dataTypes= { menuId: 'S',
			  title: 'S',
			  category: 'S',
			  parent: 'S',
			  childs: 'SS',
			  depth: 'N' }
		*/
	     //var differences = diff(origAttribues, result.attrs);
	     /*var updateRes = {
		orig: origAttribues,
		new: result.attrs,
		diff: differences
	     }*/
	     if (t == 'put') {
	        const item = self.initItem(attrs);
	        self._after.emit('create', item);
	        if (data != undefined) {
		   OldImage = internals.printObjectEvent(null, data, dataTypes, null);
		   res['OldImage'] = OldImage;
		}
		NewImage = internals.printObjectEvent(null, item.attrs, dataTypes, null);
		res['NewImage'] = NewImage;
	        var differences = diff(res['NewImage'], OldImage);
	        if (differences != undefined) {
                   self._after.emit('itemUpdated', res);
	        }
         	return callback(null, item);
	      } else if (t == 'delete') {
     
	        let item = null;
         	if (data.Attributes) {
	          item = self.initItem(self.serializer.deserializeItem(data.Attributes));
	        }
		delete res['NewImage'];
		OldImage = internals.printObjectEvent(null, origAttribues, dataTypes, null);
		res['OldImage'] = OldImage;
		     
	        self._after.emit('destroy', item);
               self._after.emit('itemUpdated', res);
	        return callback(null, item);
	     } else if (t == 'update') {
	       var differences = diff(res['NewImage'], res['OldImage']);
               self._after.emit('update', result);
	       if (differences != undefined) {
                   self._after.emit('itemUpdated', res);
	       }
               return callback(null, result);
	     } 
           });
       });
}

internals.printObjectEvent = function(k, r, dataTypes, dt) {

	var j = {};

	if (dt != undefined && dt == 'S') {
		j[dt]=r;
		return j;
	}
	if (dt != undefined && dt == 'N') {
		j[dt]=r;
		return j;
	}

	if (dt != undefined && dt == 'SS') {
		var innerArr = [];
	        for (var z = 0, arrlenInner = r.length; z < arrlenInner; z++) {
	            // skip loop if the property is from prototype
	            innerArr.push(internals.printObjectEvent(null, r[z], dataTypes, undefined));
		    //innerArr.splice(z, 0, internals.printObjectEvent(null, r[z], dataTypes, undefined));
	       }
	       j[dt]=innerArr;
 	       return j;
	} 
	if (dt != undefined && dt == 'M') {
		//TODO:
	}

		if( Object.prototype.toString.call( r ) === '[object Array]' ) {
			//array ??
		} else if( Object.prototype.toString.call( r ) === '[object Object]' ) {
  		  for (var key in r) {
		  // skip loop if the property is from prototype
	        	if (!r.hasOwnProperty(key)) continue;
	                var obj = r[key];
			j[key]=internals.printObjectEvent(key, obj, dataTypes, dataTypes[key]);
		  }
		  return j;
		} else {
			return r;
		}


}

internals.updateExpressions = (schema, data, options) => {
  const exp = expressions.serializeUpdateExpression(schema, data);

  if (options.UpdateExpression) {
    const parsed = expressions.parse(options.UpdateExpression);

    exp.expressions = _.reduce(parsed, (result, val, key) => {
      if (!_.isEmpty(val)) {
        result[key] = result[key].concat(val);
      }

      return result;
    }, exp.expressions);
  }

  if (_.isPlainObject(options.ExpressionAttributeValues)) {
    exp.values = _.merge({}, exp.values, options.ExpressionAttributeValues);
  }

  if (_.isPlainObject(options.ExpressionAttributeNames)) {
    exp.attributeNames = _.merge({}, exp.attributeNames, options.ExpressionAttributeNames);
  }

  return _.merge({}, {
    ExpressionAttributeValues: exp.values,
    ExpressionAttributeNames: exp.attributeNames,
    UpdateExpression: expressions.stringify(exp.expressions),
  });
};

Table.prototype.update = function (tmpItem, options, callback) {
  const self = this;
  console.error("UPDATE ITEM TEST:");
        
  var item = {};
  if( Object.prototype.toString.call( tmpItem ) === '[object Array]' ) {
      var ll = tmpItem.length;
      item = [];
      for(var i = 0; i < ll; i++) {
        var arrItem = tmpItem[i];
        var arItm = {};
        for(var k in arrItem) {
           if (arrItem.hasOwnProperty(k)) {
                if (!(arrItem[k] === null || arrItem[k] == undefined || arrItem[k] == '')) {
                        arItm[k] = arrItem[k];
                }
          }
       }
       item.push(arItm);
     }
  } else {
      for(var k in tmpItem) {
        if (tmpItem.hasOwnProperty(k)) {
                if (!(tmpItem[k] === null || tmpItem[k] == undefined || tmpItem[k] == '')) {
                        item[k] = tmpItem[k];
                }
        }
     }
  }
  console.error("update using");
  console.error(item);


  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  const start = callback => {
    const paramName = _.isString(self.schema.updatedAt) ? self.schema.updatedAt : 'updatedAt';

    if (self.schema.timestamps && self.schema.updatedAt !== false && !_.has(item, paramName)) {
      item[paramName] = new Date().toISOString();
    }

    return callback(null, item);
  };

  internals.callBeforeHooks(self, 'update', start, (err, data) => {
    if (err) {
      return callback(err);
    }

    const hashKey = data[self.schema.hashKey];
    let rangeKey = data[self.schema.rangeKey];

    if (_.isUndefined(rangeKey)) {
      rangeKey = null;
    }

    let params = {
      TableName: self.tableName(),
      Key: self.serializer.buildKey(hashKey, rangeKey, self.schema),
      ReturnValues: 'ALL_NEW'
    };

    let exp = null;
    try {
      exp = internals.updateExpressions(self.schema, data, options);
    } catch (e) {
      return callback(e);
    }

    if (exp.UpdateExpression) {
      params.UpdateExpression = exp.UpdateExpression;
      delete options.UpdateExpression;
    }

    if (exp.ExpressionAttributeValues) {
      params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
      delete options.ExpressionAttributeValues;
    }

    if (exp.ExpressionAttributeNames) {
      params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
      delete options.ExpressionAttributeNames;
    }

    if (options.expected) {
      internals.addConditionExpression(params, options.expected);
      delete options.expected;
    }

    params = _.chain({}).merge(params, options).omitBy(_.isEmpty).value();

    var q = {};
    q[self.schema.hashKey] = params.Key[self.schema.hashKey];
    if (this.docClient.service.config.isLocal) {
        internals.simulateAwsDynamoDbEvents(params, self, callback,'update');
    } else {
       self.sendRequest('update', params, (err, data) => {
         if (err) {
           return callback(err);
         }
   
         let result = null;
         if (data.Attributes) {
           result = self.initItem(self.serializer.deserializeItem(data.Attributes));
         }
   
         self._after.emit('update', result);
         return callback(null, result);
       });
    }
  });
};

internals.addConditionExpression = (params, expectedConditions) => {
  _.each(expectedConditions, (val, key) => {
    let operator;
    let expectedValue = null;

    const existingValueKeys = _.keys(params.ExpressionAttributeValues);

    if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === true) {
      operator = 'attribute_exists';
    } else if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === false) {
      operator = 'attribute_not_exists';
    } else if (_.isObject(val) && _.has(val, '<>')) {
      operator = '<>';
      expectedValue = _.get(val, '<>');
    } else {
      operator = '=';
      expectedValue = val;
    }

    const condition = expressions.buildFilterExpression(key, operator, existingValueKeys, expectedValue, null);
    params.ExpressionAttributeNames = _.merge({}, condition.attributeNames, params.ExpressionAttributeNames);
    params.ExpressionAttributeValues = _.merge({}, condition.attributeValues, params.ExpressionAttributeValues);

    if (_.isString(params.ConditionExpression)) {
      params.ConditionExpression = `${params.ConditionExpression} AND (${condition.statement})`;
    } else {
      params.ConditionExpression = `(${condition.statement})`;
    }
  });
};

Table.prototype.destroy = function (hashKey, rangeKey, options, callback) {
  const self = this;

  if (_.isPlainObject(rangeKey) && typeof options === 'function' && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof rangeKey === 'function' && !callback) {
    callback = rangeKey;
    options = {};
    rangeKey = null;
  } else if (_.isPlainObject(rangeKey) && !callback) {
    callback = options;
    options = rangeKey;
    rangeKey = null;
  } else if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  if (_.isPlainObject(hashKey)) {
    rangeKey = hashKey[self.schema.rangeKey];

    if (_.isUndefined(rangeKey)) {
      rangeKey = null;
    }

    hashKey = hashKey[self.schema.hashKey];
  }

  let params = {
    TableName: self.tableName(),
    Key: self.serializer.buildKey(hashKey, rangeKey, self.schema)
  };

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);

    delete options.expected;
  }

  params = _.merge({}, params, options);

    if (self.docClient.service.config.isLocal) {
        internals.simulateAwsDynamoDbEvents(params, self, callback,'delete', null, null);
    } else {
       self.sendRequest('delete', params, (err, data) => {
         if (err) {
           return callback(err);
         }
     
         let item = null;
         if (data.Attributes) {
           item = self.initItem(self.serializer.deserializeItem(data.Attributes));
         }
     
         self._after.emit('destroy', item);
         return callback(null, item);
       });
    }
};

Table.prototype.query = function (hashKey) {
  const self = this;

  return new Query(hashKey, self, self.serializer);
};

Table.prototype.scan = function () {
  const self = this;

  return new Scan(self, self.serializer);
};

Table.prototype.parallelScan = function (totalSegments) {
  const self = this;

  return new ParallelScan(self, self.serializer, totalSegments);
};


internals.deserializeItems = (table, callback) => (err, data) => {
  if (err) {
    return callback(err);
  }

  const result = {};
  if (data.Items) {
    result.Items = _.map(data.Items, i => table.initItem(table.serializer.deserializeItem(i)));

    delete data.Items;
  }

  if (data.LastEvaluatedKey) {
    result.LastEvaluatedKey = data.LastEvaluatedKey;

    delete data.LastEvaluatedKey;
  }

  return callback(null, _.merge({}, data, result));
};

Table.prototype.runQuery = function (params, callback) {
  const self = this;

  self.sendRequest('query', params, internals.deserializeItems(self, callback));
};

Table.prototype.runScan = function (params, callback) {
  const self = this;

  self.sendRequest('scan', params, internals.deserializeItems(self, callback));
};

Table.prototype.runBatchGetItems = function (params, callback) {
  const self = this;
  self.sendRequest('batchGet', params, callback);
};

internals.attributeDefinition = (schema, key) => {
  let type = schema._modelDatatypes[key];

  if (type === 'DATE') {
    type = 'S';
  }

  return {
    AttributeName: key,
    AttributeType: type
  };
};

internals.keySchema = (hashKey, rangeKey) => {
  const result = [{
    AttributeName: hashKey,
    KeyType: 'HASH'
  }];

  if (rangeKey) {
    result.push({
      AttributeName: rangeKey,
      KeyType: 'RANGE'
    });
  }

  return result;
};

internals.secondaryIndex = (schema, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: params.name,
    KeySchema: internals.keySchema(schema.hashKey, params.rangeKey),
    Projection: projection
  };
};

internals.globalIndex = (indexName, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: indexName,
    KeySchema: internals.keySchema(params.hashKey, params.rangeKey),
    Projection: projection,
    ProvisionedThroughput: {
      ReadCapacityUnits: params.readCapacity || 1,
      WriteCapacityUnits: params.writeCapacity || 1
    }
  };
};

Table.prototype.createTable = function (options, callback) {
  const self = this;

  console.error("IIIINNNN CREATE TABLE");
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }
  const attributeDefinitions = [];

  attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.hashKey));

  if (self.schema.rangeKey) {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, self.schema.rangeKey));
  }

  const localSecondaryIndexes = [];

  _.forEach(self.schema.secondaryIndexes, params => {
    attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    localSecondaryIndexes.push(internals.secondaryIndex(self.schema, params));
  });

  const globalSecondaryIndexes = [];

  _.forEach(self.schema.globalIndexes, (params, indexName) => {
    if (!_.find(attributeDefinitions, { AttributeName: params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.hashKey));
    }

    if (params.rangeKey && !_.find(attributeDefinitions, { AttributeName: params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(self.schema, params.rangeKey));
    }

    globalSecondaryIndexes.push(internals.globalIndex(indexName, params));
  });

  const keySchema = internals.keySchema(self.schema.hashKey, self.schema.rangeKey);

  const params = {
    AttributeDefinitions: attributeDefinitions,
    TableName: self.tableName(),
    KeySchema: keySchema,
    ProvisionedThroughput: {
      ReadCapacityUnits: options.readCapacity || 1,
      WriteCapacityUnits: options.writeCapacity || 1
    }
  };

  if (localSecondaryIndexes.length >= 1) {
    params.LocalSecondaryIndexes = localSecondaryIndexes;
  }

  if (globalSecondaryIndexes.length >= 1) {
    params.GlobalSecondaryIndexes = globalSecondaryIndexes;
  }

  self.sendRequest('createTable', params, callback);
};

Table.prototype.describeTable = function (callback) {
  const params = {
    TableName: this.tableName(),
  };

  console.error("IIIINNNN DECRIBE TABLE");
  this.sendRequest('describeTable', params, callback);
};

Table.prototype.deleteTable = function (callback) {
  callback = callback || _.noop;

  const params = {
    TableName: this.tableName(),
  };

  this.sendRequest('deleteTable', params, callback);
};

Table.prototype.updateTable = function (throughput, callback) {
  const self = this;
  if (typeof throughput === 'function' && !callback) {
    callback = throughput;
    throughput = {};
  }

  callback = callback || _.noop;
  throughput = throughput || {};

  async.parallel([
    async.apply(internals.syncIndexes, self),
    async.apply(internals.updateTableCapacity, self, throughput),
  ], callback);
};

internals.updateTableCapacity = (table, throughput, callback) => {
  const params = {};

  if (_.has(throughput, 'readCapacity') || _.has(throughput, 'writeCapacity')) {
    params.ProvisionedThroughput = {};

    if (_.has(throughput, 'readCapacity')) {
      params.ProvisionedThroughput.ReadCapacityUnits = throughput.readCapacity;
    }

    if (_.has(throughput, 'writeCapacity')) {
      params.ProvisionedThroughput.WriteCapacityUnits = throughput.writeCapacity;
    }
  }

  if (!_.isEmpty(params)) {
    params.TableName = table.tableName();
    table.sendRequest('updateTable', params, callback);
  } else {
    return callback();
  }
};

internals.syncIndexes = (table, callback) => {
  callback = callback || _.noop;

  table.describeTable((err, data) => {
    if (err) {
      return callback(err);
    }

    const missing = _.values(internals.findMissingGlobalIndexes(table, data));
    if (_.isEmpty(missing)) {
      return callback();
    }

    // UpdateTable only allows one new index per UpdateTable call
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.html#GSI.OnlineOps.Creating
    const maxIndexCreationsAtaTime = 5;
    async.mapLimit(missing, maxIndexCreationsAtaTime, (params, callback) => {
      const attributeDefinitions = [];

      if (!_.find(attributeDefinitions, { AttributeName: params.hashKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.hashKey));
      }

      if (params.rangeKey && !_.find(attributeDefinitions, { AttributeName: params.rangeKey })) {
        attributeDefinitions.push(internals.attributeDefinition(table.schema, params.rangeKey));
      }

      const currentWriteThroughput = data.Table.ProvisionedThroughput.WriteCapacityUnits;
      const newIndexWriteThroughput = _.ceil(currentWriteThroughput * 1.5);
      params.writeCapacity = params.writeCapacity || newIndexWriteThroughput;

      table.log.info('adding index %s to table %s', params.name, table.tableName());

      const updateParams = {
        TableName: table.tableName(),
        AttributeDefinitions: attributeDefinitions,
        GlobalSecondaryIndexUpdates: [{ Create: internals.globalIndex(params.name, params) }]
      };

      table.sendRequest('updateTable', updateParams, callback);
    }, callback);
  });
};

internals.findMissingGlobalIndexes = (table, data) => {
  if (_.isNull(data) || _.isUndefined(data)) {
    // table does not exist
    return table.schema.globalIndexes;
  } else {
    const indexData = _.get(data, 'Table.GlobalSecondaryIndexes');
    const existingIndexNames = _.map(indexData, 'IndexName');

    const missing = _.reduce(table.schema.globalIndexes, (result, idx, indexName) => {
      if (!_.includes(existingIndexNames, idx.name)) {
        result[indexName] = idx;
      }

      return result;
    }, {});

    return missing;
  }
};
