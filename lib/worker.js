var _ = require('underscore');
var redis = require('redis');
var url = require('url');

var Worker = module.exports = function (options) {
  options || (options = {});
  if (options.redisUrl) this.connect(options.redisUrl, options.redisOptions);
  if (options.key) this.key = options.key;
  this.jobs = options.jobs || {};
};

_.extend(Worker.prototype, {
  key: 'jobby',

  waitingKey: function () { return this.key + ':waiting'; },

  workingKey: function () { return this.key + ':working'; },

  start: function (cb) { return this.fetch(cb); },

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
    if (job.t > new Date()) return this.addAndRemove(job, val, cb);
    var self = this;
    this.jobs[job.n](job.o, function (er) {
      if (er) {
        if (cb) cb(er);
        return self.pushAndRemove(job, val, cb);
      }
      self.remove(val, cb);
    });
    return this.fetch(cb);
  },

  addAndRemove: function (job, val, cb) {
    var self = this;
    this.push(job.n, job.o, function (er) {
      if (er) {
        if (cb) cb(er);
        return self.addAndRemove(job, val, cb);
      }
      self.remove(val, cb);
    });
    return this;
  },

  remove: function (val, cb) {
    var self = this;
    this.pusher.lrem(this.workingKey(), -1, val, function (er) {
      if (er) {
        if (cb) cb(er);
        self.remove(val, cb);
      }
      cb();
    });
    return this;
  }
});
