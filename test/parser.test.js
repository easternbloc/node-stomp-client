var util = require('sys'),
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
  }
  
});
