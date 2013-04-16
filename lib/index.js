var _ = require('underscore');
var Worker = require('./worker');

module.exports = _.extend(new Worker(), {Worker: Worker});
