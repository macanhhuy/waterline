
/**
 * Module Dependencies
 */

var _ = require('lodash'),
    async = require('async'),
    utils = require('../../utils/helpers'),
    hasOwnProperty = utils.object.hasOwnProperty;

/**
 * Builds up a set of operations to perform based on search criteria.
 *
 * This allows the ability to do cross-adapter joins as well as fake joins
 * on adapters that haven't implemented the join interface yet.
 */

var Operations = module.exports = function(context, criteria, parent) {

  // Set context
  this.context = context;

  // Set criteria
  this.criteria = criteria;

  // Set parent
  this.parent = parent;

  // Build Up Operations
  this.operations = this._buildOperations();

  return this;
};


/***********************************************************************************
 * PUBLIC METHODS
 ***********************************************************************************/


/**
 * Run Operations
 *
 * Execute a set of generated operations returning an array of results that can
 * joined in-memory to build out a valid results set.
 *
 * @param {Function} cb
 * @api public
 */

Operations.prototype.run = function run(cb) {

  var self = this;

  // Create array to hold results
  var queryResults = [];

  // Grab the parent operation, it will always be the very first operation
  var parentOp = this.operations.shift();

  // Run The Parent Operation
  this._runOperation(parentOp.adapter.query._adapter, parentOp.method, parentOp.criteria, function(err, results) {

    if(err) return cb(err);

    // Add the parent results to the queryResults array for use later when doing the
    // in-memory join of populated results.
    queryResults.push({ parent: results });

    self._execChildOpts(queryResults[0].parent, function(err, childResults) {
      if(err) return cb(err);
      queryResults = queryResults.concat(childResults);

      // Join the results together into a single level
      var joinedResults = self._joinResults(queryResults);
      cb(null, joinedResults);
    });

  });

};


/***********************************************************************************
 * PRIVATE METHODS
 ***********************************************************************************/


/**
 * Build up the operations needed to perform the query based on criteria.
 *
 * @return {Array}
 * @api private
 */

Operations.prototype._buildOperations = function _buildOperations() {

  var self = this,
      operations = [];

  // Check if joins were used, if not only a single operation is needed. In this case joins will
  // continue to be attached to the criteria and the adapter should be able to understand that and
  // use it when querying.
  if(!hasOwnProperty(this.criteria, 'joins')) {

    operations.push({
      adapter: this.context._adapter,
      method: this.parent,
      criteria: this.criteria
    });

    return operations;
  }

  // Breakout all the joins needed, arranged by adapter
  var adapters = this._findAdapters();

  // Check if the parent collection supports joins or not
  if(this.context._adapter.hasJoin()) {

    // If the parent collection supports joins, see if there are any joins we can combine
    // into the lookup. To do this first check for any adapter identities that match then if
    // there is also a matching config file add the joins for that adapter into the parent and
    // remove the child from the adapters object.
    var parentAdapter = this.context.adapter.identity;
    var parentConfig = this.context._adapter.config;

    Object.keys(adapters).forEach(function(adapter) {
      if(adapter !== parentAdapter) return;

      var testAdapter = _.cloneDeep(adapters[adapter]);

      // See if there is a matching config
      testAdapter.forEach(function(item, idx) {
        if(!hasOwnProperty(item, 'config')) return;
        if(item.config !== parentConfig) return;

        // This is a match so create an operation for the parent record that includes
        // the joins for this adapter with this config.

        // Remove original joins
        var criteria = _.cloneDeep(self.criteria);
        delete criteria.joins;

        // Attach adapter/config specific joins
        criteria.joins = item.joins;

        // Create the operation
        operations.push({
          adapter: self.context._adapter,
          method: 'join',
          criteria: criteria
        });

        // Remove the joins set from the adapters object, it's no longer needed and can be
        // run on the same query as the parent.
        adapters[adapter] = adapters[adapter].splice(idx, 1);
      });
    });
  }

  // If there were no matching config objects, create the parent operation but remove the joins.
  // This will happen when all the records being populated live in a different adapter or use a
  // different connection config.
  if(operations.length === 0) {

    // Remove original joins
    var criteria = _.cloneDeep(self.criteria);
    delete criteria.joins;

    operations.push({
      adapter: self.context._adapter,
      method: self.parent,
      criteria: criteria
    });
  }


  // So now we have the parent operation, next we need to build up operations for any joins left
  // that may be on a different adapter or different connection on the same adapter. These can't
  // be run until after the parent operation has been run because they will rely on data returned
  // from that operation. This allows you to have for example your users in postgresql and your
  // preferences in mongo and populate preferences when a user is queried on.

  // Build up operations for each adapter used
  Object.keys(adapters).forEach(function(adapter) {
    adapters[adapter].forEach(function(config) {

      // Using the joins for this config, build up a criteria object that can be
      // used to find the data that's needed. It will need to be a placeholder for an
      // IN query that can be populated by primary keys from the parent query.

      // If there are multiple joins, go through them and see if any junction tables are used.
      // If junction tables are used, check if the adapter can support joins and if so you can
      // use a single operation. If not, multiple operations need to be done and then joined
      // in-memory by linking the child to another operation.

      if(config.joins.length === 1) {
        operations.push({
          collection: config.collection,
          method: 'find',
          joins: config.joins
        });
        return;
      }

      // Check if the adapter supports joins
      if(config.collection._adapter.hasJoin()) {
        var criteria = {};
        criteria.joins = _.cloneDeep(config.joins);
        operations.push({ collection: config.collection, method: 'find', joins: config.joins });
        return;
      }

      // Check if junction tables are used and if so link the operation to another
      // operation on the parent key
      config.joins.forEach(function(join) {
        if(join.junctionTable) {
          operations.push({
            collection: config.collection,
            method: 'find',
            joins: [join],
            parent: join.parent
          });
          return;
        }

        operations.push({
          collection: config.collection,
          method: 'find',
          joins: [join]
        });
      });

    });
  });

  return operations;
};

/**
 * Build up a set of adapters needed for the query.
 *
 * It should return an object that has the adapter identities as key names and an array
 * of join trees grouped by adapter config.
 *
 * @return {Array}
 * @api private
 */

Operations.prototype._findAdapters = function _findAdapters() {

  var self = this,
      adapters = {};

  // For each join, look at the adapter and see if it supports joins and combine operations
  // on the same adapters. If a join relies on data from other joins build up trees that can
  // be used when operations are run to pass results from one operation down to the next.
  this.criteria.joins.forEach(function(join) {

    // If this join is a junctionTable, find the parent operation and add it to that tree
    // instead of creating a new operation on another adapter. This allows cross-adapter
    // many-to-many joins to be used where the join relies on the results of the parent operation
    // being run first.

    if(join.junctionTable) {

      // Grab the parent collection
      var collection = self.context.waterline.collections[join.parent];

      // Ensure the object value for this adapter's identity is an array
      adapters[collection.adapter.identity] = adapters[collection.adapter.identity] || [];

      adapters[collection.adapter.identity].forEach(function(item) {
        item.joins.forEach(function(currentJoin) {
          if(currentJoin.child !== join.parent) return;
          currentJoin.children = currentJoin.children || [];
          currentJoin.children.push(join);
        });
      });

      return;
    }

    var child = join.child;
    var collection = self.context.waterline.collections[child];

    // Ensure the object value for this adapter's identity is an array
    adapters[collection.adapter.identity] = adapters[collection.adapter.identity] || [];

    // Store an array of objects with each representing a config for the adapter

    // If there are no objects in the array lets push the first one
    if(adapters[collection.adapter.identity].length === 0) {

      adapters[collection.adapter.identity].push({
        config: collection.adapter.config,
        collection: collection,
        joins: [join]
      });

      return;
    }

    // Objects already exist on the adapter so we need to compare config objects and see
    // if any match. If not add a new object to the array.
    adapters[collection.adapter.identity].forEach(function(item, idx) {

      // If the config objects match using a strict equality we can add the join value to
      // the tree or build another join
      if(item.config === collection.adapter.config) {

        // Check if this is a junctionTable, if so it needs to be appended to it's
        // matching join as a tree structure.
        // if(!join.junctionTable) {

          adapters[collection.adapter.identity][idx].joins.push(join);
          return;

        // }

        // We need to find the matching join and add the item to the tree. This could be coming
        // from a different adapter so we need to look in all the adapters.
        // var joinCollection = self.context.waterline.collections[join.parent];
        // console.log(joinCollection)
        // // Find the matching join and add to the tree
        // // adapters[collection.adapter.identity][idx].joins.forEach(function(currentJoin) {
        // // adapters[joinCollection.adapter.identity][idx].joins.forEach(function(currentJoin) {
        // adapters[joinCollection.adapter.identity].forEach(function(item2, idx) {
        //   if(item.config !== joinCollection.adapter.config) return;

        //   console.log('got here')
        // });
        // //   if(currentJoin.child !== join.parent) return;
        // //   currentJoin.children = currentJoin.children || [];
        // //   currentJoin.children.push(join);
        // // });

        return;
      }

      adapters[collection.adapter.identity].push({
        config: collection.adapter.config,
        collection: collection,
        joins: [join]
      });
    });
  });

  return adapters;
};

/**
 * Run An Operation
 *
 * Performs an operation and runs a supplied callback.
 *
 * @param {Object} adapter
 * @param {String} method
 * @param {Object} criteria
 * @param {Function} cb
 *
 * @api private
 */

Operations.prototype._runOperation = function _runOperation(adapter, method, criteria, cb) {

  // Run the parent operation
  adapter[method](criteria, cb);

};

/**
 * Execute Child Operations
 *
 * If joins are used and an adapter doesn't support them, there will be child operations that will
 * need to be run. Parse each child operation and run them along with any tree joins and return
 * an array of children results that can be combined with the parent results.
 *
 * @param {Array} parentResults
 * @param {Function} cb
 */

Operations.prototype._execChildOpts = function _execChildOpts(parentResults, cb) {

  var self = this;

  // Build up a set of child operations that will need to be run
  // based on the results returned from the parent operation.
  this._buildChildOpts(parentResults, function(err, opts) {

    if(err) return cb(err);

    var optResults = {};

    // Run the generated operations in parallel
    async.each(opts, function(item, next) {

      self._collectChildResults(item, function(err, results) {
        if(err) return next(err);
        _.merge(optResults, results);
        next();
      });

    }, function(err) {

      if(err) return cb(err);
      cb(null, optResults);

    });
  });

};

/**
 * Build Child Operations
 *
 * Using the results of a parent operation, build up a set of operations that contain criteria
 * based on what is returned from a parent operation. These can be arrays containing more than
 * one operation for each child, which will happen when "join tables" would be used.
 *
 * Each set should be able to be run in parallel.
 *
 * @param {Array} parentResults
 * @param {Function} cb
 * @return {Array}
 * @api private
 */

Operations.prototype._buildChildOpts = function _buildChildOpts(parentResults, cb) {

  var opts = [];

  // Build up operations that can be run in parallel using the results of the parent operation
  async.each(this.operations, function(item, next) {

    // Build up
    var localOpts = [];

    // Check if the operation has children operations. If so we need to traverse
    // the tree and pass the results of each one to the child. Used in junctionTable
    // operations where the adapter doesn't support native joins.
    //
    // If no child operations are present just build up a single operation to perform.

    item.joins.forEach(function(join) {

      var parents = [],
          idx = 0;

      // Go through all the parent records and build up an array of keys to look in. This
      // will be used in an IN query to grab all the records needed for the "join".
      parentResults.forEach(function(result) {

        if(!hasOwnProperty(result, join.parentKey)) return;
        parents.push(result[join.parentKey]);

      });

      // If no parents match the join criteria, don't build up an operation
      if(parents.length === 0) return;

      // Build up criteria that will be used inside an IN query
      var criteria = {};
      criteria[join.childKey] = parents;

      // Build a simple operation to run with criteria from the parent results.
      // Give it an ID so that children operations can reference it if needed.
      localOpts.push({
        id: idx,
        collection: item.collection.waterline.collections[join.child],
        method: item.method,
        criteria: criteria,
        join: join
      });

      // If there are children records, add the opt but don't add the criteria
      if(join.children) {
        join.children.forEach(function(child) {

          // Build a simple opt to run with criteria from the parent results
          localOpts.push({
            collection: item.collection.waterline.collections[child.child],
            method: item.method,
            parent: idx,
            join: join
          });

          idx++;
        });
      }
    });

    // Add the localOpts to the child opts array
    opts.push(localOpts);

    next();

  }, function(err) {
    cb(err, opts);
  });
};

/**
 * Collect Child Operation Results
 *
 * Run a set of child operations and return the results in a namespaced array
 * that can later be used to do an in-memory join.
 *
 * @param {Array} opts
 * @param {Function} cb
 * @api private
 */

Operations.prototype._collectChildResults = function _collectChildResults(opts, cb) {

  var self = this,
      optResults = [];

  if(!opts || opts.length === 0) return cb(null, {});

  // Run the operations and any child operations in series so that each can access the
  // results of the previous operation.
  async.eachSeries(opts, function(opt, next) {

    // Alias under a key so we can combine them in-memory later
    var alias;

    if(opt.join.model) {
      alias = opt.join.parentKey + ':' + opt.join.childKey;
    } else {
      alias = opt.join.alias + '_' + opt.join.child + '__' + opt.join.parentKey + ':' + opt.join.childKey;
    }

    // If the operation has a parent, look into the optResults and build up a criteria
    // object using the results of a previous operation
    if(hasOwnProperty(opt, 'parent')) {

      var parents = [];

      // Normalize to array
      if(!Array.isArray(optResults[opt.parent])) optResults[opt.parent] = [optResults[opt.parent]];

      optResults[opt.parent].forEach(function(result) {

        if(!result.hasOwnProperty(opt.join.parentKey)) return;
        parents.push(result[opt.join.parentKey]);

      });

      var criteria = {};
      criteria[opt.join.childKey] = parents;

      return self._runOperation(opt.collection._adapter, opt.method, criteria, function(err, values) {
        if(err) return next(err);
        var obj = {};
        obj[alias] = values;
        optResults.push(obj);
        next();
      });
    }

    self._runOperation(opt.collection._adapter, opt.method, opt.criteria, function(err, values) {
      if(err) return next(err);
      var obj = {};
      obj[alias] = values;
      optResults.push(obj);
      next();
    });

  }, function(err) {

    if(err) return cb(err);

    // If the optResults have more than a single item in them it means there are child
    // operations and there should be some sort of in-memory join done before pushing these
    // results to the queryResults. This usually only happens on many-to-many operations
    // where a junction table is needed.

    if(optResults.length === 1) {
      return cb(null, optResults[0]);
    }

    var joinsNeeded = {},
        joinedResults = {};

    // In Memory Join of Child Operations

    // Build up a mapping of things to join
    opts.forEach(function(opt) {
      if(!hasOwnProperty(opt, 'parent')) return;

      // Map the first in-memory join, for now it will only ever be 1 level deep
      joinsNeeded[opt.join.childKey] = {
        parent: opt.join.children[0].parentKey,
        child: opt.join.children[0].childKey,
        alias: opt.join.children[0].alias + '_' + opt.join.children[0].child + '_' + opt.join.children[0].parentKey
      };

    });

    if(!Array.isArray(optResults[0])) return cb(null, optResults[0]);

    // Loop through each result item and build up objects
    optResults[0].forEach(function(record) {
      Object.keys(record).forEach(function(key) {

        if(Object.keys(joinsNeeded).indexOf(key) < 0) return;

        var generatedKey = key + '__' + record[key];
        joinedResults[generatedKey] = joinedResults[generatedKey] || [];

        // For each of the child results see if any match the criteria
        var join = joinsNeeded[key];

        optResults[1].forEach(function(result) {
          if(!hasOwnProperty(result, join.child)) return;
          if(result[join.child] !== record[join.parent]) return;
          joinedResults[generatedKey].push(result);
        });
      });
    });

    cb(null, joinedResults);
  });

};

/**
 * Join Results
 *
 * Takes a set of parent and child results and combines them into a single level.
 * A unique naming convention is used on the key names where a __ indicates a key
 * name on the matching parent result and a : seperates the parent key name and the
 * child key name.
 *
 * @param {Array} optResults
 * @return {Array}
 * @api private
 */

Operations.prototype._joinResults = function _joinResults(optResults) {

  // Copy the parents from the optResults into a new array. It will always be the first
  // item and namespaced under parent.
  var parents = _.cloneDeep(optResults[0].parent);

  // Remove the parents from the optResults
  optResults.shift();

  // For each set of results, merge the children onto matching parent records.
  optResults.forEach(function(join) {

    // For each key being joined search the parent records
    Object.keys(join).forEach(function(key) {

      var generatedKey,
          alias,
          childKey,
          parentKey,
          values;

      // A results array should only have a single key
      generatedKey = key;

      // Check if key has an alias
      if(generatedKey.split('__').length > 1) {
        alias = generatedKey.split('__')[0];
        generatedKey = generatedKey.split('__')[1];
      }

      // Grab the child and parent key
      parentKey = generatedKey.split(':')[0];
      childKey = generatedKey.split(':')[1];

      // Grab the key values
      values = join[key];

      // For Each child, find any matching parents
      values.forEach(function(child) {

        // See if any matching parent records exist
        parents.forEach(function(parent) {
          if(!hasOwnProperty(parent, parentKey)) return;
          if(parent[parentKey] !== child[childKey]) return;

          // If an alias exists, this is a collection and will contain an array of child records
          if(alias) {
            parent[alias] = parent[alias] || [];
            parent[alias].push(child);
          } else {
            parent[parentKey] = child;
          }
        });
      });
    });
  });

  return parents;
};
