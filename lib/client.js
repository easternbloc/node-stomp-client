var net = require('net');
var util = require('util');
var events = require('events');
var StompFrame = require('./frame').StompFrame;
var StompFrameEmitter = require('./parser').StompFrameEmitter;

// Inbound frame validators
var StompFrameCommands = {
  '1.0': {
    'CONNECTED': {
      'headers': { 'session': { required: true } }
    },
    'MESSAGE' : {
      'headers': {
        'destination': { required: true },
        'message-id': { required: true }
      }
    },
    'ERROR': {},
    'RECEIPT': {}
  }
};

function StompClient(address, port, user, pass, protocolVersion) {
  events.EventEmitter.call(this);
  this.user = (user || '');
  this.pass = (pass || '');
  this.address = (address || '127.0.0.1');
  //TODO Check what the default stomp port is
  this.port = (port || 2098);
  this.version = (protocolVersion || '1.0');
  this.errorCallbacks = [];
  this.subscriptions = {};
  this._stompFrameEmitter = new StompFrameEmitter(StompFrameCommands[this.version]);
  return this;
}

util.inherits(StompClient, events.EventEmitter);

StompClient.prototype.connect = function (connectedCallback, errorCallback) {
  var self = this;

  if (Object.prototype.toString.call(errorCallback) === '[object Function]') {
    this.errorCallbacks.push(errorCallback);
  }

  self.stream = net.createConnection(self.port, self.address);
  self.stream.on('connect', function() {
    self.onConnect();
  });

  self.stream.on('error', self.emit.bind(self, 'error'));

  self.on('connect', function(sessionId) {
    connectedCallback(sessionId);
  });
};

StompClient.prototype.disconnect = function (disconnectedCallback, errorCallbackToRemove) {
  var self = this,
      errorCallbackIndex;

  if (this.stream) {

    this.on('disconnect', function () {

      if (errorCallbackToRemove) {
        errorCallbackIndex = this.errorCallbacks.indexOf(errorCallbackToRemove);
        this.errorCallbacks.splice(errorCallbackIndex, 1);
      }

      disconnectedCallback();
    });

    var frame = new StompFrame({
      command: 'DISCONNECT'
    }).send(this.stream);

    process.nextTick(function() {
      self.stream.emit('end');
    });

  }
};

StompClient.prototype.onConnect = function() {

  var self = this;

  // First set up the frame parser
  var frameEmitter = self._stompFrameEmitter;

  self.stream.on('data', function(data) {
    frameEmitter.handleData(data);
  });

  self.stream.on('end', function() {
    self.stream.end();
    process.nextTick(function() {
    self.emit('disconnect');
    });

  });

  frameEmitter.on('MESSAGE', function(frame) {
      self.subscriptions[frame.headers.destination].map(function(callback) {
        callback(frame.body, frame.headers);
      });
  });

  frameEmitter.on('CONNECTED', function(frame) {
    self.emit('connect', frame.headers.session);
  });

  frameEmitter.on('ERROR', function(frame) {
    util.log(frame.headers.message + ' (frame body: ' + frame.body + ')');
    for (var i = 0; i < self.errorCallbacks.length; i++) {
      self.errorCallbacks[i](frame.headers, frame.body);
    }
  });

  frameEmitter.on('parseError', function(err) {
    var msg = 'Error Parsing Message: ' + err['message'];
    if (err.hasOwnProperty('details')) {
      msg += ' (' + err['details'] + ')';
    }
    util.log(msg);
  });

  // Send the CONNECT frame
  var frame = new StompFrame({
    command: 'CONNECT',
    headers: {
      'login': self.user,
      'passcode': self.pass
    }
  }).send(self.stream);
};

StompClient.prototype.subscribe = function(queue, callback, headers) {
  headers && (headers["destination"] = queue) || (headers = {"destination": queue});
  if (!(queue in this.subscriptions)) {
    this.subscriptions[queue] = [];
    new StompFrame({
      command: 'SUBSCRIBE',
      headers: headers
    }).send(this.stream);
  }
  this.subscriptions[queue].push(callback);
};

// no need to pass a callback parameter as there is no acknowledgment for successful UNSUBSCRIBE from the STOMP server
StompClient.prototype.unsubscribe = function (queue, headers) {
  headers && (headers["destination"] = queue) || (headers = {"destination": queue});
  new StompFrame({
    command: 'UNSUBSCRIBE',
    headers: headers
  }).send(this.stream);
  delete this.subscriptions[queue];
};

StompClient.prototype.publish = function(queue, message) {
  new StompFrame({
    command: 'SEND',
    headers: {
      destination: queue
    },
    body: message
  }).send(this.stream);
};

function SecureStompClient(address, port, user, pass, credentials) {
  events.EventEmitter.call(this);
  var self = this;
  self.user = user;
  self.pass = pass;
  self.subscriptions = {};
  self.stream = net.createConnection(port, address);
  self.stream.on('connect', function() {
    self.stream.setSecure(credentials);
  });
  self.stream.on('secure', function() {
    self.onConnect();
  });
}

util.inherits(SecureStompClient, StompClient);

exports.StompClient = StompClient;
exports.SecureStompClient = SecureStompClient;
