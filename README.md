Stomp Client
===========

[![Build Status](https://img.shields.io/travis/oobj/node-stomp-client.svg?style=flat-square)](http://travis-ci.org/oobj/node-stomp-client)
[![Monthly Downloads](https://img.shields.io/npm/dm/stomp-client.svg?style=flat-square)](https://www.npmjs.com/package/stomp-client)
[![Version](https://img.shields.io/npm/v/stomp-client.svg?style=flat-square)](https://www.npmjs.com/package/stomp-client)
[![Licence](https://img.shields.io/npm/l/stomp-client.svg?style=flat-square)](https://github.com/easternbloc/node-stomp-client/blob/master/LICENSE)

A node.js [STOMP](http://stomp.github.com) client. Props goes to [Russell
Haering](https://github.com/russellhaering/node-stomp-broker) for doing the
initial legwork.

The following enhancements have been added:

*   Unit tests
*   Ability to support different protocol versions (1.0 or 1.1) - more work needed
*   Inbound frame validation (required / regex'able header values)
*   Support for UNSUBSCRIBE frames in client
*   Ability to add custom headers to SUBSCRIBE/UNSUBSCRIBE frames
*   ACK and NACK support

## Installation

	npm install stomp-client

## Super basic example

    var Stomp = require('stomp-client');
    var destination = '/queue/someQueueName';
    var client = new Stomp('127.0.0.1', 61613, 'user', 'pass');

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

## Queue Names

The meaning of queue names is not defined by the STOMP spec, but by the Broker.
However, with ActiveMQ, they should begin with `"/queue/"` or with `"/topic/"`, see
[STOMP1.0](http://stomp.github.io/stomp-specification-1.0.html#frame-SEND) for
more detail.

## Stomp = require('stomp-client')

Require returns a constructor for STOMP client instances.

For backwards compatibility, `require('stomp-client').StompClient` is also
supported.

## Stomp(address, [port], [user], [pass], [protocolVersion], [vhost], [reconnectOpts], [tls])

- `address`: address to connect to, default is `"127.0.0.1"`
- `port`: port to connect to, default is `61613`
- `user`: user to authenticate as, default is `""`
- `pass`: password to authenticate with, default is `""`
- `protocolVersion`: see below, defaults to `"1.0"`
- `vhost`: see below, defaults to `null`
- `reconnectOpts`: see below, defaults to `{}`
- `tls`: Establish a tls/ssl connection.  If an object is passed for this argument it will passed as options to the tls module.

Protocol version negotiation is not currently supported and version `"1.0"` is
the only supported version.

ReconnectOpts should contain an integer `retries` specifying the maximum number
of reconnection attempts, and a `delay` which specifies the reconnection delay.
 (reconnection timings are calculated using exponential backoff. The first reconnection
 happens immediately, the second reconnection happens at `+delay` ms, the third at `+ 2*delay` ms, etc).

## Stomp(options)

- `options`: Properties are named the same as the positional parameters above. The property 'host' is accepted as an alias for 'address'.

## stomp.connect([callback, [errorCallback]])

Connect to the STOMP server. If the callbacks are provided, they will be
attached on the `'connect'` and `'error'` event, respectively.

## virtualhosts

If using virtualhosts to namespace your queues, you must pass a `version` header of '1.1' otherwise it is ignored.

## stomp.disconnect(callback)

Disconnect from the STOMP server. The callback will be executed when disconnection is complete.
No reconnections should be attempted, nor errors thrown as a result of this call.

## stomp.subscribe(queue, [headers,] callback)

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

## stomp.ack(messageId, subscription, [transaction]),
## stomp.nack(messageId, subscription, [transaction])

- `messageId`: the id of the message to ack/nack
- `subscription`: the id of the subscription
- `transaction`: optional transaction name

## Property: `stomp.publishable` (boolean)
Returns whether or not the connection is currently writable. During normal operation
this should be true, however if the client is in the process of reconnecting,
this will be false.

## Event: `'connect'`

Emitted on successful connect to the STOMP server.

## Event: `'error'`

Emitted on an error at either the TCP or STOMP protocol layer. An Error object
will be passed. All error objects have a `.message` property, STOMP protocol
errors may also have a `.details` property.

If the error was caused by a failure to reconnect after exceeding the number of
reconnection attempts, the error object will have a `reconnectionFailed` property.

## Event: `'reconnect'`

Emitted when the client has successfully reconnected. The event arguments are
the new `sessionId` and the reconnection attempt number.

## Event: `'reconnecting'`

Emitted when the client has been disconnected for whatever reason, but is going
to attempt to reconnect.

## Event: `'message'` (body, headers)

Emitted for each message received. This can be used as a simple way to receive
messages for wildcard destination subscriptions that would otherwise not trigger
the subscription callback.

## LICENSE

[MIT](LICENSE)
