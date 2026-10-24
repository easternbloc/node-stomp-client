var testCase  = require('nodeunit').testCase,
    StompFrameEmitter = require('../lib/parser').StompFrameEmitter;

var validators = {
  MESSAGE: {
    headers: {
      'destination': { required: true },
      'message-id': { required: true }
    }
  }
};

function frame(id, body, withContentLength) {
  var headers = 'MESSAGE\ndestination:/queue/test\nmessage-id:' + id + '\n';
  if (withContentLength !== false) {
    headers += 'content-length:' + Buffer.byteLength(body) + '\n';
  }
  return headers + '\n' + body + '\0';
}

module.exports = testCase({

  setUp: function(callback) {
    callback();
  },

  tearDown: function(callback) {
    callback();
  },

  'parses a multi-byte UTF-8 body when content-length is present and data arrives byte by byte': function(test) {
    var emitter = new StompFrameEmitter(validators);
    var body = '{"unit":"\u00b0C","name":"h\u00e9llo"}';
    var wire = Buffer.from(frame('1', body), 'utf8');
    var received = [];

    emitter.on('MESSAGE', function(f) {
      received.push(f);
    });

    for (var i = 0; i < wire.length; i++) {
      emitter.handleData(wire.slice(i, i + 1));
    }

    test.equal(received.length, 1, 'one frame should have been emitted');
    test.equal(received[0].body, body, 'body should be decoded without data loss');
    test.equal(emitter.buffer.length, 0, 'no unparsed data should remain');
    test.equal(emitter.state, 0, 'parser should be back in COMMAND state');
    test.done();
  },

  'parses a multi-byte UTF-8 body without content-length': function(test) {
    var emitter = new StompFrameEmitter(validators);
    var body = 'h\u00e9llo \u00b0C';
    var wire = Buffer.from(frame('1', body, false), 'utf8');
    var received = [];

    emitter.on('MESSAGE', function(f) {
      received.push(f);
    });

    for (var i = 0; i < wire.length; i++) {
      emitter.handleData(wire.slice(i, i + 1));
    }

    test.equal(received.length, 1);
    test.equal(received[0].body, body);
    test.equal(emitter.buffer.length, 0);
    test.done();
  },

  'parses several frames delivered in a single chunk': function(test) {
    var emitter = new StompFrameEmitter(validators);
    var bodies = ['{"t":"\u00b0C"}', 'plain ascii', '{"t":"\u00e9"}'];
    var wire = Buffer.from(bodies.map(function(body, i) {
      return frame(String(i), body);
    }).join('\n'), 'utf8');
    var received = [];

    emitter.on('MESSAGE', function(f) {
      received.push(f);
    });

    emitter.handleData(wire);

    test.equal(received.length, 3);
    test.deepEqual(received.map(function(f) { return f.body; }), bodies);
    test.equal(emitter.buffer.length, 0);
    test.done();
  },

  'parses a binary-safe body containing a NULL octet when content-length is present': function(test) {
    var emitter = new StompFrameEmitter(validators);
    var body = Buffer.from([0x61, 0x00, 0x62, 0xc2, 0xb0, 0x63]); // a\0b°c
    var wire = Buffer.concat([
      Buffer.from('MESSAGE\ndestination:/queue/test\nmessage-id:1\ncontent-length:' + body.length + '\n\n', 'ascii'),
      body,
      Buffer.from([0x00])
    ]);
    var received = [];

    emitter.on('MESSAGE', function(f) {
      received.push(f);
    });

    emitter.handleData(wire);

    test.equal(received.length, 1);
    test.equal(received[0].body, body.toString('utf8'));
    test.equal(emitter.buffer.length, 0);
    test.done();
  }

});
