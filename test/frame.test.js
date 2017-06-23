var util = require('util'),
    Events = require('events').EventEmitter,
    nodeunit  = require('nodeunit'),
    testCase  = require('nodeunit').testCase,
    StompFrame = require('../lib/frame').StompFrame;

// Mock net object so we never try to send any real data
var connectionObserver = new Events();
connectionObserver.writeBuffer = [];
connectionObserver.write = function(data) {
  this.writeBuffer.push(data);
};

module.exports = testCase({

  setUp: function(callback) {
    callback();
  },

  tearDown: function(callback) {
    connectionObserver.writeBuffer = [];
    callback();
  },

  'test StompFrame utility methods work correctly': function (test) {
    var frame = new StompFrame({
      'command': 'HITME',
      'headers': {
        'header1': 'value1',
        'header2': 'value2'
      },
      'body': 'wewp de doo'
    });

    test.equal(frame.command, 'HITME');

    // setCommand
    frame.setCommand('SOMETHINGELSE');
    test.equal(frame.command, 'SOMETHINGELSE');

    // setHeader
    frame.setHeader('header2', 'newvalue');
    test.equal(frame.headers['header2'], 'newvalue');

    frame.setHeader('new-header', 'blah');
    test.equal(frame.headers['new-header'], 'blah');

    // TODO - Content-length assignment? Why is this done?

    // appendToBody
    frame.appendToBody('pip pop');
    test.equal(frame.body, 'wewp de doopip pop');

    test.done();
  },

  'test stream writes are correct on arbitrary frame definition': function (test) {
    var frame = new StompFrame({
      'command': 'HITME',
      'headers': {
        'header1': 'value1',
        'header2': 'value2'
      },
      'body': Buffer.from('wewp de doo')
    });

    // Command before headers, content-length auto-inserted, and terminating with null char (line feed chars for each line too)
    var expectedStream = [
      'HITME\n',
      'header1:value1\n',
      'header2:value2\n',
      'content-length:11\n',
      '\n',
      'wewp de doo',
      '\u0000'
    ];



    frame.send(connectionObserver);

    test.deepEqual(expectedStream.join(''), connectionObserver.writeBuffer.join(''), 'frame stream data is correctly output on the mocked wire');
    test.done();
  },

  'check validation of arbitrary frame with arbitrary frame construct': function (test) {
    var validation,
        frameConstruct = {
          'headers': {
            'blah': { required: true },
            'regexheader': { required: true, regex: /(wibble|wobble)/ }
          }
        };

    var frame = new StompFrame({
      'command': 'COMMAND',
      'headers': {},
      'body': ''
    });

    // Invalid header (required)
    validation = frame.validate(frameConstruct);
    test.equal(validation.isValid, false);
    test.equal(validation.message, 'Header "blah" is required for COMMAND');
    test.equal(validation.details, 'Frame: {"command":"COMMAND","headers":{},"body":""}');
    frame.setHeader('blah', 'something or other');  // Set it now so it doesn't complain in later tests

    // Invalid header (regex)
    frame.setHeader('regexheader', 'not what it should be');
    validation = frame.validate(frameConstruct);
    test.equal(validation.isValid, false);
    test.equal(validation.message, 'Header "regexheader" has value "not what it should be" which does not match against the following regex: /(wibble|wobble)/ (Frame: {"command":"COMMAND","headers":{"blah":"something or other","regexheader":"not what it should be"},"body":""})');

    // Now make the header valid
    frame.setHeader('regexheader', 'wibble');
    validation = frame.validate(frameConstruct);
    test.equal(validation.isValid, true);

    frame.setHeader('regexheader', 'wobble');
    validation = frame.validate(frameConstruct);
    test.equal(validation.isValid, true, 'still valid!');

    test.done();
  },
  'test content-length header is present when suppress-content-length is not': function(test) {
    var frame = new StompFrame({
        'command': 'SEND',
        'body' : Buffer.from('Content length is 20')
    });
    frame.send(connectionObserver);

    //Check the headers for the content-length header
    var writtenString = connectionObserver.writeBuffer.join('');
    var containsContentLengthHeader = (writtenString.split("\n").indexOf("content-length:20") == -1 ? false : true);
    test.equal(containsContentLengthHeader, true, "Content length header should exist since we are not suppressing it");

    test.done();
  },
  'test content-length is not present when suppress-content-length is provided': function(test) {
    var frame = new StompFrame({
        'command': 'SEND',
        'headers': {
          'suppress-content-length': true
        },
        'body' : 'Content length is 20'
    });
    frame.send(connectionObserver);

    //Check the headers for the content-length header
    var writtenString = connectionObserver.writeBuffer.join('');
    var containsContentLengthHeader = (writtenString.split("\n").indexOf("content-length:20") == -1 ? false : true);
    test.equal(containsContentLengthHeader, false, "Content length header should not exist since we are suppressing it");
    test.done();
  },
  'test content-length is not present when frame.body is string': function(test) {
    var frame = new StompFrame({
        'command': 'SEND',
        'headers': {},
        'body' : 'Content length is 20'
    });
    frame.send(connectionObserver);

    //Check the headers for the content-length header
    var writtenString = connectionObserver.writeBuffer.join('');
    var containsContentLengthHeader = (writtenString.split("\n").indexOf("content-length:20") == -1 ? false : true);
    test.equal(containsContentLengthHeader, false, "Content length header should not exist since we are suppressing it");
    test.done();
  },
  'test stream write correctly handles single-byte UTF-8 characters as a buffer': function(test) {
      var frame = new StompFrame({
          'command': 'SEND',
          'body' : Buffer.from('Welcome!')
      });
      frame.send(connectionObserver);

      var writtenString = connectionObserver.writeBuffer.join('');
      //Assume content-length header is second line
      var contentLengthHeaderLine = writtenString.split("\n")[1];
      var contentLengthValue = contentLengthHeaderLine.split(":")[1].trim();

      test.equal(Buffer.byteLength(frame.body), contentLengthValue, "We should be truthful about how much data we plan to send to the server");

      test.done();
  },
  'test stream write correctly handles single-byte UTF-8 characters as a string': function(test) {
      var frame = new StompFrame({
          'command': 'SEND',
          'body' : 'Welcome!'
      });
      frame.send(connectionObserver);

      var expectedStream = [
        'SEND\n\n',
        'Welcome!',
        '\0'
      ];

      var writtenString = connectionObserver.writeBuffer.join('');
      
      test.equal(expectedStream.join(''), writtenString, 'Sent content should be eqaul to expected content');

      test.done();
  },
  'test stream write correctly handles multi-byte UTF-8 characters as a buffer': function(test) {
      var frame = new StompFrame({
          'command': 'SEND',
          'body' : Buffer.from('Ẇḗḽḉớḿẽ☃')
      });
      frame.send(connectionObserver);

      var writtenString = connectionObserver.writeBuffer.join('');
      //Assume content-length header is second line
      var contentLengthHeaderLine = writtenString.split("\n")[1];
      var contentLengthValue = contentLengthHeaderLine.split(":")[1].trim();

      test.equal(Buffer.byteLength(frame.body), contentLengthValue, "We should be truthful about how much data we plan to send to the server");

      test.done();
  },
  'test stream write correctly handles multi-byte UTF-8 characters as a string': function(test) {
      var frame = new StompFrame({
          'command': 'SEND',
          'body' : 'Ẇḗḽḉớḿẽ☃'
      });
      frame.send(connectionObserver);

      var expectedStream = [
        'SEND\n\n',
        'Ẇḗḽḉớḿẽ☃',
        '\0'
      ];

      var writtenString = connectionObserver.writeBuffer.join('');
      
      test.equal(expectedStream.join(''), writtenString, 'Sent content should be eqaul to expected content');

      test.done();
  }

});
