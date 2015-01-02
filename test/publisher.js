'use strict';

var queue = require('../');
var publisher = new queue.Publisher({
	publisherSocketAddr: 'tcp://127.0.0.1:9997',
	partitions: 10,
});

setInterval(function(){
	//console.log('sending work');
	for (var j=0;j<10;j++) {
	  var payload = [];
	  for (var i=0;i<10;i++) {
	  	payload.push({a:'a', b: Math.random()});
	  }
	  publisher.send('topic1', payload);
 	}
}, 1000);
