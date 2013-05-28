var util = require('util'),
    Events = require('events').EventEmitter,
    nodeunit  = require('nodeunit'),
    testCase  = require('nodeunit').testCase;

var StompClient = require('../lib/client').StompClient;

// surpress logs for the test
util.log = function() {};

module.exports = testCase({

  'check connect to closed port errors': function(test) {
    var stompClient = new StompClient({port: 4});

    stompClient.connect(function() {});

    stompClient.once('error', function(er) {
      test.done();
    });
  },
});
