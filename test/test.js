'use strict';

var queue = require('../');

/* Publisher */

var p = new queue.Publisher({
	url: "amqp://192.168.1.131",
	exchangeName: "ex1",
	exchangeType: "fanout",
	retryCount: 5,
	retryDelay: 1000,

});
var count = 0;
setInterval(function() {
  count++;
  console.log("publishing message.");
  p.publish(JSON.stringify({id: count, data: "text"}), function(err) {
    if (err) {
      console.log("error publishing message.", err);
    }
  });
}, 2000);

/* ------------------------------ */


/* Consumers */

var c = new queue.Consumer({
	url: "amqp://192.168.1.131",
    exchangeName: "ex1",
    exchangeType: "fanout",
    queueName: '', //leave blank for an auto generated name. recommend when creating an exclusive queue.
    exclusive: true, //make the queue exclusive.
    durable: false,
    autoDelete: true,
    queuePattern: null, //fanout exchanges get all messages, so we dont need to bind with a pattern.
    retryCount: -1, //keep trying to connect forever.
    handler: processMessage
});

c.on('error', function(err) {
	console.log("c1 emitted fatal error.")
    console.log(err);
    process.exit(1);
});


var c2 = new queue.Consumer({
	url: "amqp://192.168.1.131",
    exchangeName: "ex1",
    exchangeType: "fanout",
    queueName: '', //leave blank for an auto generated name. recommend when creating an exclusive queue.
    exclusive: true, //make the queue exclusive.
    durable: false,
    autoDelete: true,
    queuePattern: null, //fanout exchanges get all messages, so we dont need to bind with a pattern.
    retryCount: -1, //keep trying to connect forever.   
    handler: processMessage2
});

c2.on('error', function(err) {
	console.log("c2 emitted fatal error.")
    console.log(err);
    process.exit(1);
});


function processMessage(message) {
  console.log("q1 consumer got message. ", message.content.toString());
  //console.log(message);
}

function processMessage2(message) {
  console.log("q2 consumer got message. ", message.content.toString());
}

process.on('SIGINT', function() { process.exit(); });
