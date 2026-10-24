var util = require('util');
var events = require('events');
var StompFrame = require('./frame').StompFrame;

var ParserStates = {
  COMMAND: 0,
  HEADERS: 1,
  BODY: 2,
  ERROR: 3
};

var LF = 0x0a;
var NULL_OCTET = 0x00;

function StompFrameEmitter(frameValidators) {
  events.EventEmitter.call(this);
  this.state = ParserStates.COMMAND;
  this.frame = new StompFrame();
  this.frames = [];
  // The wire protocol is byte oriented: keep the unparsed data as a Buffer so
  // that `content-length` (which is expressed in bytes) can be honoured even
  // when the payload contains multi-byte UTF-8 characters.
  this.buffer = Buffer.alloc(0);
  this.frameValidators = frameValidators || {};
  this.commands = Object.keys(this.frameValidators);
}

util.inherits(StompFrameEmitter, events.EventEmitter);

StompFrameEmitter.prototype.incrementState = function () {
  if (this.state === ParserStates.BODY || this.state === ParserStates.ERROR) {
    this.state = ParserStates.COMMAND;
  } else {
    this.state++;
  }
};

StompFrameEmitter.prototype.handleData = function (data) {
  if (!Buffer.isBuffer(data)) {
    data = Buffer.from(data, 'utf8');
  }
  this.buffer = Buffer.concat([this.buffer, data]);
  do {
    if (this.state === ParserStates.COMMAND) {
      this.parseCommand();
    }
    if (this.state === ParserStates.HEADERS) {
      this.parseHeaders();
    }
    if (this.state === ParserStates.BODY) {
      this.parseBody();
    }
    if (this.state === ParserStates.ERROR) {
      this.parseError();
    }
  } while (this.state === ParserStates.COMMAND && this.hasLine());
};

StompFrameEmitter.prototype.hasLine = function () {
  return (this.buffer.indexOf(LF) > -1);
};

StompFrameEmitter.prototype.popLine = function () {
  var index = this.buffer.indexOf(LF);
  var line = this.buffer.slice(0, index).toString('utf8');
  this.buffer = this.buffer.slice(index + 1);
  return line;
};

StompFrameEmitter.prototype.error = function (err) {
  this.emit('parseError', err);
  this.state = ParserStates.ERROR;
};

StompFrameEmitter.prototype.parseCommand = function () {
  while (this.hasLine()) {
    var line = this.popLine();
    if (line !== '') {
      if (this.commands.indexOf(line) === -1) {
        this.error({
          message: 'No such command ',
          details: 'Unrecognized Command \'' + line + '\''
        });
        break;
      }
      this.frame.setCommand(line);
      this.incrementState();
      break;
    }
  }
};

StompFrameEmitter.prototype.parseHeaders = function () {
  while (this.hasLine()) {
    var line = this.popLine();
    if (line === '') {
      this.incrementState();
      break;
    } else {
      var kv = line.split(':');
      if (kv.length < 2) {
        this.error({
          message: 'Error parsing header',
          details: 'No ":" in line "' + line + '"'
        });
        break;
      }
      this.frame.setHeader(kv[0], kv.slice(1).join(':'));
    }
  }
};

StompFrameEmitter.prototype.parseBody = function () {
  if (this.frame.contentLength > -1) {
    // Wait until the complete body and its trailing NULL octet are buffered.
    if (this.buffer.length < this.frame.contentLength + 1) {
      return;
    }
    this.frame.appendToBody(this.buffer.slice(0, this.frame.contentLength).toString('utf8'));
    this.buffer = this.buffer.slice(this.frame.contentLength);
    this.frame.contentLength = -1;
  }

  var index = this.buffer.indexOf(NULL_OCTET);

  if (index === -1) {
    // The body is not complete yet: leave it in the buffer so it can be
    // decoded in one go once the NULL terminator arrives.
    return;
  }

  // The end of the frame has been identified, finish creating it
  this.frame.appendToBody(this.buffer.slice(0, index).toString('utf8'));
  this.emitFrame();
  this.buffer = this.buffer.slice(index + 1);
};

StompFrameEmitter.prototype.emitFrame = function () {
  var frameValidation = this.getFrameValidation(this.frame.command);

  if (frameValidation.isValid) {
    // Emit the frame and reset
    this.emit('frame', this.frame);             // Event emit to catch any frame emission
    this.emit(this.frame.command, this.frame);  // Specific frame emission
  } else {
    this.emit('parseError', {
      message: frameValidation.message,
      details: frameValidation.details,
    });
  }

  this.frame = new StompFrame();
  this.incrementState();
};

StompFrameEmitter.prototype.getFrameValidation = function (command) {
  if (!this.frameValidators.hasOwnProperty(command)) {
    this.emit('parseError', { message: 'No validator defined for ' + command });
  } else {
    return this.frame.validate(this.frameValidators[command]);
  }
};

StompFrameEmitter.prototype.parseError = function () {
  var index = this.buffer.indexOf(NULL_OCTET);
  if (index > -1) {
    this.buffer = this.buffer.slice(index + 1);
    this.incrementState();
  } else {
    this.buffer = Buffer.alloc(0);
  }
};

exports.StompFrameEmitter = StompFrameEmitter;
