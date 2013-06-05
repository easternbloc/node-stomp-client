function StompFrame(frame) {
  if (frame === undefined) {
    frame = {};
  }
  this.command = frame.command || '';
  this.headers = frame.headers || {};
  this.body = frame.body || '';
  this.contentLength = -1;
}

StompFrame.prototype.toString = function() {
  return JSON.stringify({
    command: this.command,
    headers: this.headers,
    body: this.body
  });
};

StompFrame.prototype.send = function(stream) {
  stream.write(this.command + '\n');
  for (var key in this.headers) {
    stream.write(key + ':' + this.headers[key] + '\n');
  }
  if (this.body.length > 0) {
    stream.write('content-length:' + this.body.length + '\n');
  }
  stream.write('\n');
  if (this.body.length > 0) {
    stream.write(this.body);
  }
  stream.write('\0');
};

StompFrame.prototype.setCommand = function(command) {
  this.command = command;
};

StompFrame.prototype.setHeader = function(key, value) {
  this.headers[key] = value;
  if (key.toLowerCase() == 'content-length') {
    this.contentLength = parseInt(value);
  }
};

StompFrame.prototype.appendToBody = function(data) {
  this.body += data;
};

StompFrame.prototype.validate = function(frameConstruct) {
  var frameHeaders = Object.keys(this.headers);

  // Check validity of frame headers
  for (var header in frameConstruct.headers) {
    var headerConstruct = frameConstruct.headers[header];

    // Check required (if specified)
    if (headerConstruct.hasOwnProperty('required') && headerConstruct.required === true) {
      if (frameHeaders.indexOf(header) === -1) {
        return {
          isValid: false,
          message: 'Header "' + header + '" is required, and missing from frame: ' + this.toString()
        };
      }
    }

    // Check regex of header value (if specified)
    if (headerConstruct.hasOwnProperty('regex') && frameHeaders.indexOf(header) > -1) {
      if (!this.headers[header].match(headerConstruct.regex)) {
        return {
          isValid: false,
          message: 'Header "' + header + '" has value "' + this.headers[header] + '" which does not match against the following regex: ' + headerConstruct.regex + ' (Frame: ' + this.toString() + ')'
        };
      }
    }
  }

  return { isValid: true };
};

exports.StompFrame = StompFrame;
