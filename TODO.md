# TODO

Here's a list of things that could be done, in no particular order.

## Receipts

Every send function that sends a frame that can have a receipt header should
also take a receipt callback, and auto-add a receipt header if callback is
passed.

The DISCONNECT header should probably always request a RECEIPT, so guarantee
flushing of data. 1.2 says you
[should](http://stomp.github.io/stomp-specification-1.2.html#DISCONNECT), and
while its not mentioned as a should in 1.0, receipt IDs are supported.

## Version 1.1 and 1.2

Version 1.1 and 1.2 support should be added, and options should allow the higher
protocol versions to be requested (note that version fallback will occur, the
newer versions fall back to 1.0).

## Wildcard topic support

Wildcards, http://activemq.apache.org/wildcards.html, should be supported, they
don't work now because the destination queue is fully qualified, so node-client
doesn't know which callback to pass the message to, this implies it needs to
implement the wildcards itself to find which subscription callback to pass
the message to.

## TLS support

TLS support should be implemented, SecureStompClient hasn't worked since node
0.4 dropped .setSecure(), but for when it gets fixed, note:
- it only half-initializes self, it should share the init code with
  StompClient
- it doesn't allow protocol version to be specified, it should probably
  just take an options object, and pass some of it to the tls methods.

## Fix the 'disconnect' event

The current connection state is not well managed, the 'disconnect' event is
fired even if we haven't called .disconnect(). It should be more like net,
if we request a disconnect, we will get a disconnect event (or an error).
If we didn't request, and get closed, we should emit an error. And either way,
we should emit close when the socket goes away, so there is always a final
event.

## Invalid output

It is possible to cause invalid headers and bodies to be sent using various
combinations of non-ascii strings, and invalid chars such as NUL, :, NL, etc.
For 1.0, some chars simply must not be used in headers, in later versions, they
can be escaped.

Also, the ActiveMQ STOMP support describes using the presence or absence of the
content-length header to decide whether the content is a string. The frame
emitter always attaches content-length, it probably should not if its known to
be a utf-8 string without embedded NUL.

## Options object

The exported StompClient should accept either (port, address, ...), or an
options object, or a URL. This can be done in a backwards compatible way.  Also,
if secure STOMP was supported, it could be requested by setting `options.secure` to
an options object acceptable to
<http://nodejs.org/docs/latest/api/tls.html#tls_tls_connect_options_callback>

## JSON support

Allows publishing objects as JSON with a content-type of application/json, and
converting incoming messages to objects based on content-type.
