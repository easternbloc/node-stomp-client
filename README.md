Stomp Client
===========

[![Build Status](https://secure.travis-ci.org/easternbloc/node-stomp-client.png)](http://travis-ci.org/easternbloc/node-stomp-client)

A node.js [STOMP](http://stomp.github.com) client. Props goes to [Russell Haering](https://github.com/russellhaering/node-stomp-broker) for doing the initial legwork.

The following enhancements have been added:

*   Unit tests
*   Ability to support different protocol versions (1.0 or 1.1) - more work needed
*   Inbound frame validation (required / regex'able header values)
*   Support for UNSUBSCRIBE frames in client
*   Ability to add custom headers to SUBSCRIBE/UNSUBSCRIBE frames

## Installation
	npm install stomp-client

## Super basic example

	var Client = require('stomp-client').StompClient;
	var destination = '/queue/someQueueName';

	var client = new Client('127.0.0.1', 2098, 'user', 'pass', '1.0');

	client.connect(function(sessionId) {
		client.subscribe(destination, function(body, headers) {
			console.log('This is the body of a message on the subscribed queue:', body);
		});

		client.publish(destination, 'Oh herrow');
	});

The client comes in two forms, a standard or secure client. The example below is using the standard client. To use the secure client simply change **StompClient** to **SecureStompClient**.

##LICENSE
[MIT](LICENSE)