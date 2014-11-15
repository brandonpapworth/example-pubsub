var r     = require('rethinkdb');
var Topic = require('./topic.js');
var Queue = require('./queue.js');

var __objToString = Object.prototype.toString;

module.exports = function Exchange (name, connOpts) {
    this.db = connOpts.db = connOpts.db || 'test';
    this.name = name;
    this.conn = null;
    this.table = r.table(name);
    this._asserted = false;

    // Bluebird's .bind ensures `this` inside our callbacks is the exchange
    var exchange = this;
    exchange.promise = r.connect(connOpts)
        .then(function (conn) {
            exchange.conn = conn;
        })
        .catch(r.Error.RqlRuntimeError, function (err) {
            console.error(err.message);
            process.exit(1);
        })
    ;
};

Exchange.prototype.topic = function (name) {
    return new Topic(this, name);
};

Exchange.prototype.queue = function (filterFunc) {
    return new Queue(this, filterFunc);
};

Exchange.prototype.fullQuery = function(filterFunc){
    return this.table.changes()('new_val').filter(function(row){
        return filterFunc(row('topic'));
    });
};

Exchange.prototype.publish = function(topicKey, payload){
    var exchange = this;
    return exchange.assertTable().then(function(){
        var topIsObj = __objToString.call(topicKey) === '[object Object]';
        var topic = topIsObj ? r.literal(topicKey) : topicKey;
        return exchange.table.filter({
            topic: topic
        }).update({
            payload: payload,
            updated_on: r.now
        }).run(exchange.conn);
    }).then(function(updateResult){
        // If the topic doesn't exist yet, insert a new document. Note:
        // it's possible someone else could have inserted it in the
        // meantime and this would create a duplicate. That's a risk we
        // take here. The consequence is that duplicated messages may
        // be sent to the consumer.
        if(updateResult.replaced === 0){
            return exchange.table.insert({
                topic: topicKey,
                payload: payload,
                updated_on: r.now()
            }).run(exchange.conn);
        }else{
            return updateResult;
        }
    });
};

Exchange.prototype.subscribe = function(filterFunc, iterFunc){
    var exchange = this;
    return exchange.assertTable().then(function(){
        return exchange.fullQuery(filterFunc).run(exchange.conn);
    }).then(function(cursor){
        cursor.each(function(err, message){
            iterFunc(message.topic, message.payload);
        });
    });
};

Exchange.prototype.assertTable = function(){
    var exchange = exchange;
    return exchange.promise.then(function(){
        if (exchange._asserted){
            return;
        }

        return r.dbCreate(exchange.db).run(exchange.conn).finally(function(){
            return r.db(exchange.db).tableCreate(exchange.name).run(exchange.conn);
        }).catch(r.Error.RqlRuntimeError, function(err){
            if(err.msg.indexOf('already exists') === -1){
                throw err;
            }
        }).then(function(){
            exchange._asserted = true;
        });
    });
};
