
var amqp = require('amqplib');
var util = require("util");

function Exchange(options) {
  this.exchange = options.name;
  this.type = options.type || "fanout";
  this.url = options.url || 'amqp://127.0.0.1';
}

module.exports = Exchange;

Exchange.prototype.ensureConnection = function(callback) {
  var self = this;
  if (this.conn) {
    return self._ensureExchange(callback);
  }
  if (self.connecting) {
    //another request is openning a connection.
    return callback(new Error("waiting for new connection."));
  }

  self.connecting = true;
  console.log("creating new connection to %s.", self.url);
  amqp.connect(self.url).then(function(conn) {
    console.log("connection to %s established.", self.url);
    self.conn = conn;
    self.connecting = false;
    conn.on('close', function() {
      self.conn = null;
    });
    return self._ensureExchange(callback);
  }, function(err) {
    // connection attempt failed.
    self.connecting = false;
    callback(err);
  });
}

Exchange.prototype._ensureExchange = function(callback) {
  var self = this;
  if (this.ch) {
    return callback();
  }
  if (self.connecting) {
    //another request is opening a connection.
    return callback(new Error("waiting for new connection."));
  }
  self.connecting = true;
  console.log("creating new channel on connection to %s.", self.url);
  this.conn.createChannel().then(function(ch) {
    self.conn.on('error', function(err) {
      console.log('connetion emitted error.', err);
      //we need to catch this error, but dont need to do anything else.
    });
    console.log("channel created on connection to %s.", self.url);
    ch.assertExchange(self.exchange, self.type, {durable: false}).then(function() {
      console.log("exchange %s available.", self.exchange);
      self.ch = ch;
      self.connecting = false;
      ch.on("close", function() {
        self.ch = null;
      });
      return callback();
    }, function(err) {
      ch.close();
      return callback(err);
    });
  }, function(err) {
    self.connecting = false;
    self.conn.close();
    self.conn = null;
    return callback(err);
  });
}

