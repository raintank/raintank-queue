'use strict';
var zmq = require('zmq');
var util = require('util');
var COUNTER = {};

function Publisher() {
	this.partitions = 10;
	this.publisherSocketAddr = 'tcp://localhost:9997';
}

var publisher = module.exports = exports = new Publisher;

Publisher.prototype.init = function(options) {
	if (!options) {
		options = {};
	}
	this.partitions = options.partitions || 10;
	this.publisherSocketAddr = options.publisherSocketAddr || 'tcp://localhost:9997';

	this.socket = zmq.socket('push');
	this.socket.connect(this.publisherSocketAddr);
	setInterval(function() {
		for (var topic in COUNTER) {
			var count = COUNTER[topic];
			COUNTER[topic] = 0;
			console.log("publishing %s msg per second to topic %s", count/10, topic);
		}
	}, 10000)
	return this;
}

Publisher.prototype.send = function(topic, partition, payload) {
	if (!payload && util.isArray(partition)) {
		payload = partition;
		partition = Math.floor(Math.random() * this.partitions);
	}
  	var msg = {
		topic: topic,
	  	partition: partition,
	  	payload: payload
	}
	if (!(COUNTER[topic])) {
		COUNTER[topic] = 0;
	}
	COUNTER[topic] += payload.length;
	this.socket.send(JSON.stringify(msg));
}



