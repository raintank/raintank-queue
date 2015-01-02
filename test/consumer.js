'use strict';

var queue = require('../');

var consumer = new queue.Consumer({
	mgmtUrl: "http://localhost:9999"
});
consumer.on('connect', function() {
	consumer.join('topic1', 'test');
});

consumer.on('message', function(topic, partition, msg) {
	//console.log("%s:%s - %j", topic, partition, msg);
});


