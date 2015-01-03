'use strict';

var io = require('socket.io-client')
var zmq = require('zmq');
var util = require('util');
var events = require("events");

var activePartitions = {};
var COUNTER = {};

function Consumer(options) {
    if (!(options)) {
        options = {};
    }
    this.mgmtUrl = options.mgmtUrl || 'http://localhost:9999';
    this.consumerSocketAddr = options.consumerSocketAddr || 'tcp://localhost:9998';
    events.EventEmitter.call(this);
    this.init();
}

util.inherits(Consumer, events.EventEmitter);

module.exports = Consumer;


Consumer.prototype.init = function() {
    var self = this;
    this.mgmtSocket = io(this.mgmtUrl, {transports: ["websocket"]});
    this.zmqSock = zmq.socket('sub');
    this.zmqSock.connect(this.subSocketAddress);

    this.mgmtSocket.on('rebalance', function(data){
        console.log('rebalancing %s.', data.topic);
        if (!(data.topic in activePartitions)) {
            activePartitions[data.topic] = [];
        }

        activePartitions[data.topic].forEach(function(part) {
            var room = util.format('%s:%s', data.topic, part);
            self.zmqSock.unsubscribe(room);
        });
        activePartitions[data.topic] = [];

        data.partitions.forEach(function(part) {
            var room = util.format('%s:%s', data.topic, part);
            activePartitions[data.topic].push(part);
            self.zmqSock.subscribe(room);
        });
        setTimeout(function() {
            self.mgmtSocket.emit('ready', data);
            self.emit('ready', data);
        }, 500)
    });

    this.mgmtSocket.on('disconnect', function(){
        console.log("disconnected from broker");
        for (var topic in activePartitions) { 
            for (var i=0; i < activePartitions[topic].length; i++) {
                var part = activePartitions[topic].splice(0,1)[0];
                var room = util.format('%s:%s', topic, part);
                self.zmqSock.unsubscribe(room);
            }
        }
        self.emit('disconnect');
    });

    this.mgmtSocket.on('connect', function(){
        self.emit('connect');
    });

    this.zmqSock.on('message', function(room, raw) {
        var msg = JSON.parse(raw.toString());
        var topicPart = room.toString();
        var topic  = topicPart.split(":")[0];
        var partition = topicPart.split(":")[1];
        if (!(topic in COUNTER)) {
            COUNTER[topic] = 0;
        }
        COUNTER[topic] += msg.length;
        self.emit('message', topic, partition, msg);
    });

    setInterval(function() {
        for (var topic in COUNTER) {
            var count = COUNTER[topic];
            COUNTER[topic] = 0;
            console.log("Comsuming %s msg per second on topic %s", count/10, topic);
        }
    }, 10000)
}

Consumer.prototype.join = function(topic, group) {
    var self = this;
    self.mgmtSocket.emit('join', {topic: topic, group: group});
}
