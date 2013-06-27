var util = require('util'),
    Events = require('events').EventEmitter,
    nodeunit  = require('nodeunit'),
    testCase  = require('nodeunit').testCase;

var StompClient = require('../lib/client').StompClient;

// surpress logs for the test
util.log = function() {};

module.exports = testCase({

  'check connect to closed port errors': function(test) {
    var stompClient = new StompClient('127.0.0.1', 4);

    stompClient.connect(function() {});

    stompClient.once('error', function(er) {
      test.done();
    });
  },

  'check that invalid protocol version errors': function(test) {
    try {
      new StompClient('127.0.0.1', null, null, null, '0.1');
    } catch(er) {
      test.done();
    }
  },
});
