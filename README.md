raintank-queue
==============

Nodejs Message queue using ZMQ and socket.io

This module provides a fast message delivery system for Nodejs that supports dynamic scaling of publishers and consumers.

The module provides three classes; Broker, Consumer, Publisher.

The Broker class provides a message broker server.  The broker server utilizes a Socket.io server for managing the consumers a 'pull' zeroMQ socket for accepting messages from publishers, and a zeroMQ 'pub' socket for send messages to consumers.


```
var queue = require('../');
//Start a broker.
var broker = new queue.Broker({
	consumerSocketAddr: 'tcp://0.0.0.0:9998',  //address that consumers will connect to.
	publisherSocketAddr: 'tcp://0.0.0.0:9997', //address that publishers will connect to.
	mgmtUrl: "http://0.0.0.0:9999",            //address the management Socket.io server should listen on.
	flushInterval: 100,                        //how long to buffer messages before sending to comsumers.
	partitions: 10                             //how man partitions the topics should be split into.
});
```
When a consumer connects and sends a 'join' message to indicate the topics that it wishes to recieve, the broker pauses message delivery and issues a 'rebalance' event to each consumer providing the list of 'topcic:partition' pairs that the consumer should start consuming from.  When rebalancing, the consumer connects to the ZeroMQ 'pub' socket on the broker using a 'sub' socket. The consumer then sends a 'subscribe' message on this socket for each 'topic:partition' pair it is responsible for. Once all consumers have rebalanced, message delivery resumes.

The consumer class provides a simple interface for users to consume messages from one or more topics.  Users should join topics whenever the consumer emits the 'connect' event.  This ensures that if consumer loses connection with the broker, due to broker restart or network issues, then the consumer will automatically re-join the topics when connectivity is restored.

Consumer can then just listen for the 'message' event and process the the provided messages.  The 'msg' argument provided to the 'message' event will always be an array of 1 or more messages.

```
var consumer = new queue.Consumer({
	mgmtUrl: "http://localhost:9999"
});
consumer.on('connect', function() {
  // consume from the the topic 'topic1' using the group 'test'.
  // consumers using the same group will have messages distributed between them.
  // consumers using different group names, will get their own copy of the messages.
	consumer.join('topic1', 'test');
});

consumer.on('message', function(topic, partition, msg) {
	console.log("%s:%s - %j", topic, partition, msg);
});
```

The Producer class provides a very simple interface for users send messages.  All messages are sent to the brokers 'pull' zeroMQ socket using a 'push' zeroMQ socket.

```
var queue = require('../');
var publisher = new queue.Publisher({
	publisherSocketAddr: 'tcp://127.0.0.1:9997',
	partitions: 10,
});

//send a message to the broker.  'partition' is optional and if omitted the message will be sent to random partition.
// 'messages' should be an array.
publisher.send('topic1', partition, messages);

```


