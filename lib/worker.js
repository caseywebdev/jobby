var _ = require('underscore');
var redis = require('redis');
var url = require('url');

var Worker = module.exports = function (options) {
  if (options) _.extend(this, options);
  if (!this.jobs) this.jobs = {};
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

  push: function (name, options, time, cb) {
    if (!cb) {
      cb = time;
      time = new Date();
    }
    time = time instanceof Date ? +time : +new Date() + time;
    var key = this.waitingKey();
    var val = JSON.stringify({t: time, n: name, o: options});
    this.pusher.lpush(key, val, cb);
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
    if (job.t > new Date()) {
      this.repush(val, cb);
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
    this.pusher.lrange(this.workingKey(), 1, 0, function (er, val) {
      if (er) {
        cb(er);
        return self.cleanup(cb);
      }
      var job = val[0] && JSON.parse(val[0]);
      if (job && job.t + self.jobTimeout > new Date()) {
        self.remove(val, cb);
        return self.cleanup(cb);
      }
      setTimeout(_.bind(self.cleanup, self), self.cleanupInterval);
    });
    return this;
  }
});
