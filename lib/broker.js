'use strict';

var _ = require('lodash');
var zmq = require('zmq');
var util = require('util');
var url = require('url');

var BUFFER = {};
var COUNTER = {};
var topics = {};

function Broker(options) {
  if (!(options)) {
    options = {};
  }
  this.consumerSocket = zmq.socket('pub');
  this.publisherSocket = zmq.socket('pull');
  this.consumerSocketAddr = options.consumerSocketAddr || 'tcp://127.0.0.1:9998';
  this.publisherSocketAddr = options.publisherSocketAddr || 'tcp://127.0.0.1:9997';
  this.flushInterval = options.flushInterval || 100;
  this.partitions = options.partitions || 10;
  this.consumerSocket.bindSync(this.consumerSocketAddr);
  this.publisherSocket.bindSync(this.publisherSocketAddr);
  this.mgmtUrl = options.mgmtUrl || 'http://0.0.0.0:9999';
  var addr = url.parse(this.mgmtUrl);

  this.app = require('http').createServer(function(req, res){
    res.writeHead(404);
    res.end('Not found.');
  });
  this.io = require('socket.io')(this.app);
  

  this.app.listen(addr.port, addr.hostname);
  this.init();
}

module.exports = Broker;

//FLUSH BUFFER.
Broker.prototype.flushBuffer = function() {
  for (var room in BUFFER) {
    var len = BUFFER[room].length;
    if (len < 1) {
      continue;
    }
    var topic = room.split(':')[0];
    var rebalancing = false;
    for (var group in topics[topic]) {
      if (topics[topic][group].rebalance) {
        //console.log('%s paused due to rebalance', room);
        rebalancing = true;
        break;
      }
    }
    if (!rebalancing) {
      if (len > 20000) {
        console.log("ratelimiting msgs for %s", room);
        len = 20000;
      }
      var data = BUFFER[room].splice(0, len);
      this.consumerSocket.send([room, JSON.stringify(data)]);
    }
  }
};

Broker.prototype.init = function() {
  var self = this;
  this.publisherSocket.on('message', function(raw) {
    var msgs = JSON.parse(raw.toString());
    if (!util.isArray(msgs)) {
      msgs = [msgs];
    }
    console.log(msgs);
    //console.log("topic: %s, partition: %s", msg.topic, msg.partition);
    msgs.forEach(function(msg) {
      var room = util.format('%s:%s', msg.topic, msg.partition);
      if (!(room in BUFFER)) {
        BUFFER[room] = [];
      }
      if (!(msg.topic in COUNTER)) {
        COUNTER[msg.topic] = 0;
      }
      if ('payload' in msg) {
        BUFFER[room].push.apply(BUFFER[room],msg.payload);
        COUNTER[msg.topic] += msg.payload.length;
      }
    });
  });

  this.io.on('connection', function (socket) {

    socket.on('join', function(data) {;
    	//console.log(data);
      if (!(data.topic in COUNTER)) {
        COUNTER[data.topic] = 0;
      }
    	if (!(data.topic in topics)) {
    		topics[data.topic] = {};
    	};
    	if (!(data.group in topics[data.topic])) {
    		topics[data.topic][data.group] = {};
        topics[data.topic][data.group].clients = [];
    	};
    	//console.log(socket);
    	topics[data.topic][data.group].clients.push(socket);
      if (!('topics' in socket)) {
        socket.topics = {};
      }
      if (!(data.topic in socket.topics)) {
        socket.topics[data.topic] = {};
      }
      socket.topics[data.topic] = {group: data.group};
      self.rebalance(data.topic, data.group);
    });

    socket.on('ready', function(data) {
      var topic = data.topic;
      var group = data.group;
      var rebalancing = false;
      console.log('%s now ready', socket.id);
      socket.topics[topic].rebalance = false;
      topics[topic][group].clients.forEach(function(sock) {
        if (sock.topics[topic].rebalance) {
          //console.log("%s still rebalancing.", sock.id);
          rebalancing = true;
        }
      });
      if (rebalancing) {
        console.log("%s:%s rebalancing", topic, group);
      } else {
        console.log("%s:%s now balanced.", topic, group);
      }
      
      topics[topic][group].rebalance = rebalancing;
    });

    socket.on('disconnect', function() {
      console.log("socket %s disconnected", socket.id);
      for (var topic in socket.topics) {
        var group = socket.topics[topic].group;
        var count = 0;
        var clients = topics[topic][group].clients;
        while (count < clients.length) {
          if (clients[count].id == socket.id) {
            var deleted = clients.splice(count, 1)[0];
            console.log('removed socket %s from %s:%s', deleted.id, topic, group);
            count = clients.length;
          }
          count++;
        }
        self.rebalance(topic, group);
      }
    }); 
  });

  this._timer = setInterval(function() {
    self.flushBuffer();
  }, self.flushInterval);
  this._counter = setInterval(function() {
    for (var topic in COUNTER) {
      var count = COUNTER[topic];
      COUNTER[topic] = 0;
      console.log("Routing %s msg per second on topic %s", count/10, topic);
    }
  }, 10000);
}

Broker.prototype.rebalance = function(topic, group) {
  topics[topic][group].rebalance = true;
  console.log('rebalancing %s:%s', topic, group);
  var parts = [];
  for (var i=0; i< this.partitions; i++) {
    parts.push(i);
  }
  var numClients = topics[topic][group].clients.length;
  topics[topic][group].clients.forEach(function(sock) {
    sock.topics[topic].rebalance = true;
    var count = parts.length/numClients;
    numClients--;
    var clientParts = parts.splice(0,count);
    console.log('%s: %s', sock.id, clientParts);
    sock.topics[topic].partitions = clientParts;
    sock.emit('rebalance', {topic: topic, group: group, partitions: clientParts});
  });
}