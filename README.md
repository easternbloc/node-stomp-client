Stomp Client
===========

[![Build Status](https://secure.travis-ci.org/easternbloc/node-stomp-client.png)](http://travis-ci.org/easternbloc/node-stomp-client)

A node.js [STOMP](http://stomp.github.com) client. Props goes to [Russell
Haering](https://github.com/russellhaering/node-stomp-broker) for doing the
initial legwork.

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

	var client = new Client('127.0.0.1', 61613, 'user', 'pass', '1.0');

	client.connect(function(sessionId) {
		client.subscribe(destination, function(body, headers) {
			console.log('This is the body of a message on the subscribed queue:', body);
		});

		client.publish(destination, 'Oh herrow');
	});

The client comes in two forms, a standard or secure client. The example below is
using the standard client. To use the secure client simply change
`StompClient` to `SecureStompClient`


# API

## require('stomp-client').StompClient(address, port, user, pass, protocolVersion)

- `address`: address to connect to, default is '127.0.0.1'
- `port`: port to connect to, default is ` 61613`
- `user`: user to authenticate as, default is `""`
- `pass`: password to authenticate with, default is `""`
- `protocolVersion`: see below, defaults to 1.0

Protocol version negotiation is not currently supported, version `1.0` is the
only supported version.

## require('stomp-client').SecureStompClient(address, port, user, pass, credentials)

The SecureStompClient probably hasn't worked since node `0.4` dropped
`socket.setSecure()` and introduced the `tls` module.

## stomp.connect([callback, [errorCallback]])

Connect to the STOMP server. If the callbacks are provided, they will be
attached on the `'connect'` and `'error'` event, respectively.

## stomp.disconnect(callback)

Disconnect from the STOMP server. The callback will be attached on the
`'disconnect'` event.

## stomp.subscribe = function(queue, [headers,] callback)

- `queue`: queue to subscribe to
- `headers`: headers to add to the SUBSCRIBE message
- `callback`: will be called with message body as first argument,
  and header object as the second argument

## stomp.unsubscribe(queue, [headers])

- `queue`: queue to unsubscribe from
- `headers`: headers to add to the UNSUBSCRIBE message

## stomp.publish(queue, message, [headers])

- `queue`: queue to publish to
- `message`: message to publish, a string or buffer
- `headers`: headers to add to the PUBLISH message

## Event: `'connect'`

Emitted on successful connect to the STOMP server.

## Event: `'disconnect'`

Emitted on successful disconnnect from the STOMP server.

## Event: `'error'`

Emitted on an error at either the TCP or STOMP protocol layer.


## LICENSE

[MIT](LICENSE)
