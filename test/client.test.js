var util = require('util'),
    Events = require('events').EventEmitter,
    nodeunit  = require('nodeunit'),
    testCase  = require('nodeunit').testCase;

var StompClient = require('../lib/client').StompClient;
var connectionObserver;

// surpress logs for the test
util.log = function() {};

// net mockage
var net = require('net');
var StompFrame = require('../lib/frame').StompFrame;

// Override StompFrame send function to allow inspection of frame data inside a test
var oldSend;
var oldCreateConnection;
var sendHook = function() {};

module.exports = testCase({

  setUp: function(callback) {
    // Mock net object so we never try to send any real data
    connectionObserver = new Events();
    this.stompClient = new StompClient('127.0.0.1', 2098, 'user', 'pass', '1.0');

    oldCreateConnection = net.createConnection;
    net.createConnection = function() {
      return connectionObserver;
    };

    oldSend = StompFrame.prototype.send;
    StompFrame.prototype.send = function(stream) {
      var self = this;
      process.nextTick(function () {
        sendHook(self);
      });
    };

    callback();
  },
  
  tearDown: function(callback) {
    delete this.stompClient;
    sendHook = function() {};
    net.createConnection = oldCreateConnection;
    StompFrame.prototype.send = oldSend;
    callback();
  },

  'check default properties are correctly set on a basic StompClient': function(test) {
    var stompClient = new StompClient();

    test.equal(stompClient.user, '');
    test.equal(stompClient.pass, '');
    test.equal(stompClient.address, '127.0.0.1');
    test.equal(stompClient.port, 2098);
    test.equal(stompClient.version, '1.0');

    test.done();
  },

  'check outbound CONNECT frame correctly follows protocol specification': function(test) {
    var self = this;
    test.expect(4);

    sendHook = function(stompFrame) {
      test.equal(stompFrame.command, 'CONNECT');
      test.deepEqual(stompFrame.headers, {
          login: 'user',
          passcode: 'pass'
      });
      test.equal(stompFrame.body, '');
      test.equal(stompFrame.contentLength, -1);

      test.done();
    };

    //start the test
    this.stompClient.connect();
    connectionObserver.emit('connect');

  },

  'check inbound CONNECTED frame parses correctly': function(test) {
    var self = this;
    var testId = '1234';

    test.expect(2); 

    sendHook = function() {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient._stompFrameEmitter.on('CONNECTED', function (stompFrame) {
      test.equal(stompFrame.command, 'CONNECTED');
      test.equal(testId, stompFrame.headers.session);
      test.done();
    });

    //start the test
    this.stompClient.connect(function() {});
    connectionObserver.emit('connect');
  },

  'check the ERROR callback fires when we receive an error frame on connection': function (test) {
    var self = this,
      expectedHeaders = {
        message: 'some test error',
        'content-length' : 18
      },
      expectedBody = 'Error message body';

    test.expect(2);

    // mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'ERROR\nmessage:' + expectedHeaders.message + '\ncontent-length:' + expectedHeaders['content-length']  + '\n\n' + expectedBody + '\0');
    };

    this.stompClient.connect(function () {
      test.ok(false, 'Success callback of connect() should not be called');
    }, function (headers, body) {
      test.deepEqual(headers, expectedHeaders, 'passed ERROR frame headers should be as expected');
      test.equal(body, expectedBody, 'passed ERROR frame body should be as expected');
      test.done();
    });

    connectionObserver.emit('connect');
  },

  'check outbound SUBSCRIBE frame correctly follows protocol specification': function(test) {
    var self = this;
    var testId = '1234';
    var destination = '/queue/someQueue';
    
    test.expect(3);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function(stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    // Once connected - subscribe to a fake queue
    this.stompClient._stompFrameEmitter.on('CONNECTED', function (stompFrame) {
      //override the sendHook so we can test the latest stompframe to be sent
      sendHook = function(stompFrame) {
        test.equal(stompFrame.command, 'SUBSCRIBE');
        test.equal(stompFrame.headers.destination, destination);
        test.equal(stompFrame.headers.id, 'blah');
        test.done();
      };

      self.stompClient.subscribe(destination, function(){
        // this callback never gets called unless the client recieves some data down the subscription
        // the point of this test is to ensure the SUBSCRIBE frame is correctly structured
        // note the use of additional id header (optional in spec) below :)
      }, { id: 'blah' });
    });

    this.stompClient.connect(function() {});
    connectionObserver.emit('connect');
  },
  
  'check the SUBSCRIBE callback fires when we receive data down the destination queue': function(test) {
    var self = this;
    var testId = '1234';
    var destination = '/queue/someQueue';
    var messageId = 1;
    var messageToBeSent = 'oh herrow!';
    
    test.expect(5);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function(stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function() {

      // Mock inbound MESSAGE frame
      sendHook = function (stompFrame) {
        self.stompClient.stream.emit('data', 'MESSAGE\ndestination:' + destination + '\nmessage-id:' + messageId + '\n\n' + messageToBeSent + '\0');
      };

      // Subscribe to a queue, and upon receipt of message (wired above) test that body/headers correctly propogate to callback
      self.stompClient.subscribe(destination, function (body, headers) {
        test.equal(body, messageToBeSent, 'Received message matches the sent one');
        test.equal(headers['message-id'], messageId);
        test.equal(headers.destination, destination);
        test.equal(self.stompClient.subscriptions[destination].length, 1, 'ensure callback was added to subscription stack');

        // Unsubscribe and ensure queue is cleared of the subscription (and related callback)
        self.stompClient.unsubscribe(destination, {});

        test.equal(typeof self.stompClient.subscriptions[destination], 'undefined', 'ensure queue is cleared of the subscription');
        test.done();
      });
        
    });

    connectionObserver.emit('connect');
  },

  'check the ERROR callback fires when we receive an error frame on subscription': function (test) {
    var self = this,
      testId = '1234',
      destination = '/queue/someQueue',
      expectedHeaders = {
        message: 'some test error',
        'content-length' : 18
      },
      expectedBody = 'Error message body',
      errorCallbackCalled = false;

    test.expect(3);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function () {

      // Mock inbound ERROR frame
      sendHook = function (stompFrame) {
        self.stompClient.stream.emit('data', 'ERROR\nmessage:' + expectedHeaders.message + '\ncontent-length:' + expectedHeaders['content-length']  + '\n\n' + expectedBody + '\0');
      };

      // make sure the error callback hasn't been called yet
      test.equal(errorCallbackCalled, false, 'ERROR callback should not have been called yet');

      // Subscribe to a queue, and upon receipt of message (wired above) test that body/headers correctly propogate to callback
      self.stompClient.subscribe(destination, function () {
        test.ok(false, 'Success callback of subscribe() should not be called');
      });

    }, function (headers, body) {
      errorCallbackCalled = true;
      test.deepEqual(headers, expectedHeaders, 'passed ERROR frame headers should be as expected');
      test.equal(body, expectedBody, 'passed ERROR frame body should be as expected');
      test.done();
    });

    connectionObserver.emit('connect');
  },

  'check outbound UNSUBSCRIBE frame correctly follows protocol specification': function (test) {
    var self = this;
    var testId = '1234';
    var destination = '/queue/someQueue';
    
    test.expect(3);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function(stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    // Once connected - unsubscribe to a fake queue
    this.stompClient._stompFrameEmitter.on('CONNECTED', function (stompFrame) {
      //override the sendHook so we can test the latest stompframe to be sent
      sendHook = function(stompFrame) {
        test.equal(stompFrame.command, 'UNSUBSCRIBE');
        test.equal(stompFrame.headers.destination, destination);
        test.equal(stompFrame.headers.id, 'specialid');
        test.done();
      };

      self.stompClient.unsubscribe(destination, { id: 'specialid' });
    });

    this.stompClient.connect(function(){});
    connectionObserver.emit('connect');
  },

  'check the ERROR callback fires when we receive an error frame when unsubscribing': function (test) {
    var self = this,
      testId = '1234',
      destination = '/queue/someQueue',
      expectedHeaders = {
        message: 'some test error',
        'content-length' : 18
      },
      expectedBody = 'Error message body',
      errorCallbackCalled = false;

    test.expect(4);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function () {

      // Mock inbound MESSAGE frame
      sendHook = function (stompFrame) {
        self.stompClient.stream.emit('data', 'MESSAGE\ndestination:' + destination + '\nmessage-id:some message id\n\nsome message body\0');
      };

      // make sure the error callback hasn't been called yet
      test.equal(errorCallbackCalled, false, 'ERROR callback should not have been called yet');

      // Subscribe to a queue, and upon receipt of message (wired above) test that body/headers correctly propogate to callback
      self.stompClient.subscribe(destination, function () {

        // Mock inbound ERROR frame
        sendHook = function (stompFrame) {
          self.stompClient.stream.emit('data', 'ERROR\nmessage:' + expectedHeaders.message + '\ncontent-length:' + expectedHeaders['content-length']  + '\n\n' + expectedBody + '\0');
        };

        test.equal(errorCallbackCalled, false, 'ERROR callback should not have been called yet');

        self.stompClient.unsubscribe(destination, { id: 'specialid' });

      });

    }, function (headers, body) {
      errorCallbackCalled = true;
      test.deepEqual(headers, expectedHeaders, 'passed ERROR frame headers should be as expected');
      test.equal(body, expectedBody, 'passed ERROR frame body should be as expected');
      test.done();
    });

    connectionObserver.emit('connect');
  },

  'check outbound SEND frame correctly follows protocol specification': function (test) {
    var self = this;
    var testId = '1234';
    var destination = '/queue/someQueue';
    var messageToBeSent = 'oh herrow!';

    test.expect(3);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function() {

      sendHook = function(stompFrame) {
        test.equal(stompFrame.command, 'SEND');
        test.deepEqual(stompFrame.headers, { destination: destination });
        test.equal(stompFrame.body, messageToBeSent);
        test.done();
      };

      self.stompClient.publish(destination, messageToBeSent);

    });

    connectionObserver.emit('connect');
  },

  'check parseError event fires when malformed frame is received': function(test) {
    var self = this;

    test.expect(1);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\n\n\n\0');
    };

    this.stompClient._stompFrameEmitter.on('parseError', function (err) {
      test.equal(err.message, 'Header "session" is required, and missing from frame: {"command":"CONNECTED","headers":{},"body":"\\n"}');
      test.done();
    });

    this.stompClient.connect(function() {});
    connectionObserver.emit('connect');
  },

  'check disconnect method correctly sends DISCONNECT frame, disconnects TCP stream, and fires callback': function (test) {
    var self = this;

    test.expect(7);

    self.stompClient.on('disconnect', function() {
      test.ok(true, 'disconnect event fired');
    });

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:blah\n\n\0');
    };

    self.stompClient.connect(function() {

      // Assert next outbound STOMP frame is a DISCONNECT
      sendHook = function (stompFrame) {
        test.equal(stompFrame.command, 'DISCONNECT');
        test.deepEqual(stompFrame.headers, {});
        test.equal(stompFrame.body, '');
      };

      self.stompClient.stream.on('end', function() {
        test.ok(true, 'tcp stream end event is fired');
      });

      // Set disconnection callback to ensure it is called appropriately
      self.stompClient.disconnect(function () {
        test.ok(true, 'disconnect callback executed');
        test.done();
      });

    });

    // Mock the TCP end call
    connectionObserver.end = function() {
      test.ok(true, 'TCP end call made');
    };

    connectionObserver.emit('connect');
  },

  'check the ERROR callback fires when we receive an error frame when disconnecting': function (test) {
    var self = this,
      testId = '1234',
      expectedHeaders = {
        message: 'some test error',
        'content-length' : 18
      },
      expectedBody = 'Error message body',
      errorCallbackCalled = false;

    test.expect(3);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function () {

      // make sure the error callback hasn't been called yet
      test.equal(errorCallbackCalled, false, 'ERROR callback should not have been called yet');

      // Mock inbound ERROR frame
      sendHook = function (stompFrame) {
        self.stompClient.stream.emit('data', 'ERROR\nmessage:' + expectedHeaders.message + '\ncontent-length:' + expectedHeaders['content-length']  + '\n\n' + expectedBody + '\0');
      };

      // Set disconnection callback to ensure it is called appropriately
      self.stompClient.disconnect(function () {
        test.ok(false, 'Success callback of disconnect() should not be called');
      });

    }, function (headers, body) {
      errorCallbackCalled = true;
      test.deepEqual(headers, expectedHeaders, 'passed ERROR frame headers should be as expected');
      test.equal(body, expectedBody, 'passed ERROR frame body should be as expected');
      test.done();
    });

    // Mock the TCP end call
    connectionObserver.end = function () {
      test.ok(true, 'TCP end call made');
    };

    connectionObserver.emit('connect');
  },

  'check the ERROR callback is removed from the client on disconnect': function (test) {
    var self = this,
      testId = '1234',
      errorCallback = function (headers, body) {
        test.ok(false, 'error callback should not be called');
      };

    test.expect(2);

    //mock that we received a CONNECTED from the stomp server in our send hook
    sendHook = function (stompFrame) {
      self.stompClient.stream.emit('data', 'CONNECTED\nsession:' + testId + '\n\n\0');
    };

    this.stompClient.connect(function () {

      // Mock inbound ERROR frame
      sendHook = function (stompFrame) {};

      self.stompClient.disconnect(function () {
        test.equal(self.stompClient.errorCallbacks.indexOf(errorCallback), -1, 'the error callback should not be stored in the client anymore');
        test.done();
      }, errorCallback);
    }, errorCallback);

    // Mock the TCP end call
    connectionObserver.end = function () {
      test.ok(true, 'TCP end call made');
    };

    connectionObserver.emit('connect');
  }

});
