var _ = require('underscore');
var redis = require('redis');
var url = require('url');

var Worker = module.exports = function (options) {
  if (options) _.extend(this, options);
  if (!this.jobs) this.jobs = {};
  this.timeouts = [];
};

_.extend(Worker.prototype, {
  key: 'jobby',

  cleanupInterval: 1000 * 60 * 60,

  jobTimeout: 1000 * 60 * 60,

  waitingKey: function () { return this.key + ':waiting'; },

  workingKey: function () { return this.key + ':working'; },

  start: function (cb) {
    this.fetch(cb);
    return this.cleanup(cb);
  },

  connect: function (_url, options) {
    var parsed = url.parse(_url);
    var port = parsed.port;
    var host = parsed.hostname;
    var auth = parsed.auth ? parsed.auth.split(':') : [];
    var password = auth[1];
    var db = (parsed.path || '').slice(1);
    _.each(['pusher', 'popper'], function (type) {
      this[type] = redis.createClient(port, host, options);
      if (password) this[type].auth(password);
      if (db) this[type].select(db);
    }, this);
    return this;
  },

  disconnect: function () {

    // The pusher can be gracefully quit (`quit()`).
    this.pusher.quit();

    // Forcibly quit (`end()`) the popper to break out of the possibly never-
    // ending `brpoplpush` command.
    this.popper.end();

    // Clear all timeouts waiting to occur. They will be set up again when jobby
    // restarts.
    _.each(this.timeouts, function (timeout) { clearTimeout(timeout); });
  },

  push: function (name, options, time, cb) {
    if (!cb) {
      cb = time;
      time = new Date();
    }
    time = time instanceof Date ? +time : +new Date() + time;
    var val = JSON.stringify({t: time, n: name, o: options});
    this.pusher.lpush(this.waitingKey(), val, cb);
    return this;
  },

  fetch: function (cb) {
    var waiting = this.waitingKey();
    var working = this.workingKey();
    var self = this;
    this.popper.brpoplpush(waiting, working, 0, function (er, val) {
      if (er) {
        if (cb) cb(er);
        return self.fetch(cb);
      }
      self.run(val, cb);
    });
    return this;
  },

  run: function (val, cb) {
    var job = JSON.parse(val);
    var offset = job.t - new Date();
    if (offset > 0) {
      this.timeouts.push(_.delay(_.bind(this.repush, this, val, cb), offset));
    } else {
      var self = this;
      this.jobs[job.n](job.o, function (er) {
        if (er) {
          if (cb) cb(er, job);
          return self.repush(val, cb);
        }
        self.remove(val, cb);
      });
    }
    return this.fetch(cb);
  },

  repush: function (val, cb) {
    var self = this;
    this.pusher.lpush(this.waitingKey(), val, function (er) {
      if (er) {
        if (cb) cb(er);
        return self.repush(val, cb);
      }
      self.remove(val, cb);
    });
    return this;
  },

  remove: function (val, cb) {
    var self = this;
    this.pusher.lrem(this.workingKey(), 1, val, function (er) {
      if (er) {
        if (cb) cb(er);
        self.remove(val, cb);
      }
    });
    return this;
  },

  cleanup: function (cb) {
    var self = this;
    this.pusher.lrange(this.workingKey(), 0, -1, function (er, vals) {
      if (er) {
        cb(er);
        return self.cleanup(cb);
      }
      var now = new Date();
      var jobTimeout = self.jobTimeout;
      _.each(vals, function (val) {
        if (JSON.parse(val).t + jobTimeout > now) self.repush(val, cb);
      });
      self.timeouts.push(
        _.delay(_.bind(self.cleanup, self, cb), self.cleanupInterval)
      );
    });
    return this;
  }
});
