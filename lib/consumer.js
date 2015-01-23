'use strict';

var Exchange = require('./rabbitExchange');
var EventEmitter = require('events').EventEmitter
var util = require('util');

var exchanges = {};

function Consumer(options) {
  this.exchangeName = options.exchangeName || '';
  this.exchangeType = options.exchangeType || 'fanout';
  this.retryCount = options.retryCount || 5;
  this.retryDelay = options.retryDelay || 1000;
  this.url = options.url || 'amqp://localhost';
  this.queueName = options.queueName || '';

  if (!('handler' in options)) {
    throw new Error('A message handler function must be provided');
  }
  if (typeof options.handler !== 'function') {
    throw new Error("message handler must be a function.");
  }

  this.handler = options.handler;
  this.queueOpts = options.queueOpts || {};
  this.consumerOpts = options.consumerOpts || {noAck: true};
  this.queuePattern = options.queuePattern || '';

  var self = this;
  if (!(self.url in exchanges)) {
    exchanges[self.url] = {};
  }

  if (self.exchangeName in exchanges[self.url]) {
    this.exchange = exchanges[self.url][self.exchangeName];
    if (this.exchange.type !== self.exchangeType) {
      throw new Error("Exchange of a different type already exists with the name ", self.exchangeName);
    }
  } else {
    var exchangeOpts = {
      name: self.exchangeName,
      type: self.exchangeType,
      url: self.url,
    }
    this.exchange = new Exchange(exchangeOpts);
    exchanges[self.url][self.exchangeName] = this.exchange;
  }
  self.retries = 0;
  self.failed = false;

  if ("exclusive" in options) {
    self.queueOpts.exclusive = options.exclusive;
  } else {
    self.queueOpts.exclusive = false;
  }
  if ("durable" in options) {
    self.queueOpts.durable = options.durable;
  } else {
    self.queueOpts.durable = false;
  }
  if ("autoDelete" in options) {
    self.queueOpts.autoDelete = options.autoDelete;
  } else {
    self.queueOpts.autoDelete = false;
  }

  // if our connection drops, we want to restart the consumer.
  self.on('restart', function(err, delay) {
    //if the consumer has already been marked as failed, then just return.
    if (self.failed) {
        return;
    }

    if (delay == null ) {
        delay = self.retryDelay;
    }

    // temporary for debugging. //
    if (err) {
        console.log(err);
    }
    //--------------------------//

    self.retries++;
    if (self.retryCount > 0 && self.retries > self.retryCount) {
        self.failed = true;
        self.emit("error", new Error("To many failures. Giving up."));
    } else {
        return setTimeout(function() {
            self._consume();
        }, delay);
      }
  });

  //start consuming.
  self.emit("restart", null, 0);
}

util.inherits(Consumer, EventEmitter);

module.exports = Consumer;

Consumer.prototype._consume = function() {
    var name = this.queueName;
    var handler = this.handler;
    var opts = this.queueOpts;

  var self = this;
  self.exchange.ensureConnection(function(err) {
    if (err) {
        //connection not yet established. This could be due to an error,
        // or because a concurrent request is also trying to establish a 
        // connection. In either case, we just wait and try again.
      return self.emit("restart", err);
    }

    // if we made it here, then we have a connection and the exchange
    // exists.
    self.exchange.ch.on("close", function() {
        //when the channel closes, emit a close event so we can restart
        // the consumer.
        return self.emit("restart", new Error("channel closed."));
    });
    
    self.exchange.ch.assertQueue(name, opts).then(function(queue) {
        //the queue now exists, so lets bind it to the exchange.
      try {
        self.exchange.ch.bindQueue(queue.queue, self.exchangeName, self.queuePattern);
      } catch(e) {
        //binding failed for some reason, perhaps the connection just dropped.
        //close the channel so that we can try again.
        return self.exchange.ch.close();
      }
      self.exchange.ch.consume(queue.queue, handler, self.consumerOpts).then(function() {
        //we are now consuming..
        self.retries = 0;
        return self.emit('ready');
      }, function(err) {
        // starting the consumer failed.  So we close the channel to trigger a restart.
        return self.exchange.ch.close();
      });
    }, function(err) {
      //assertQueue failed.  This is a fatal error, and we wont try and connect again.
      self.failed = true;
      self.emit("error", err);
    });
  });
}
