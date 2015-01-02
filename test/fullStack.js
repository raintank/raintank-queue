'use strict';

var queue = require('../');

var broker = new queue.Broker({
	consumerSocketAddr: 'tcp://0.0.0.0:9998', //address that consumers will connect to.
	publisherSocketAddr: 'tcp://0.0.0.0:9997', //address that publishers will connect to.
	mgmtUrl: "http://0.0.0.0:9999", //port the management Socket.io server should listen on.
	flushInterval: 100, //how long to buffer messages before sending to comsumers.
	partitions: 10 //how man partitions the topic should be split into.
});;

var consumer = new queue.Consumer({
	mgmtUrl: "http://localhost:9999"
});

var publisher = queue.Publisher;
publisher.init({
	publisherSocketAddr: 'tcp://127.0.0.1:9997',
	partitions: 10,
});

consumer.on('connect', function() {
	consumer.join('topic2', 'test');
});

consumer.on('message', function(topic, partition, msg) {
	//console.log("%s:%s - %j", topic, partition, msg);
});

setInterval(function(){
	//console.log('sending work');
	for (var j=0;j<10;j++) {
	  var payload = [];
	  //for (var i=0;i<10;i++) {
	  	payload.push({a:'a', b: Math.random()});
	  //}
	  publisher.send('topic1', j, payload);
 	}
}, 0);

var t = require('./test');
t.test();