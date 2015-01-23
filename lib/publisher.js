'use strict';

var Exchange = require('./rabbitExchange');

var exchanges = {};

function Publisher(options) {
	this.exchangeName = options.exchangeName || '';
	this.exchangeType = options.exchangeType || 'fanout';
  this.retryCount = options.retryCount || 5;
  this.retryDelay = options.retryDelay || 1000;
  this.url = options.url || 'amqp://localhost';
  var self = this;
  if (!(self.url in exchanges)) {
  	exchanges[self.url] = {};
  }
  if (self.exchangeName in exchanges[self.url]) {
    this.exchange = exchanges[self.url][options.exchangeName];
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
}

module.exports = Publisher;

Publisher.prototype.publish = function(message, routing_key, callback) {
  if (typeof routing_key === 'function') {
    callback = routing_key;
    routing_key = '';
  }
  this._publish(new Buffer(message), routing_key, callback);
}

Publisher.prototype._publish = function(message, routing_key, callback, retries) {
  var self = this;
  if  (retries === undefined) {
    retries = 0;
  }
  retries++;
  self.exchange.ensureConnection(function(err) {
    if (! err) {
      try {
        self.exchange.ch.publish(self.exchangeName, routing_key, message);
      } catch(e) {
        err = e;
      }
    }
    if (err) {
      if (retries < self.retryCount) {
        setTimeout(function() {
          self._publish(message, routing_key, callback, retries);
        }, self.retryDelay);
      } else {
      	//too many errors, giving up.
        callback(err);
      }
    } else {
	  //message successfully published. :)     
      callback();
    }
  });
}
