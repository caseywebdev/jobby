# Jobby

YET ANOTHER REDIS JOB QUEUE!

## Install

```bash
npm install jobby
```

## Usage

```js
var jobby = require('jobby');

var errorHandler = function (er, job) { console.error(er, job); };

jobby.jobs.mail = function (options, cb) {
  // Do mail-ish things...
  cb();
};

jobby
  .connect(redisUrl)
  .start(errorHandler);
  .push('mail', {to: 'a', from: 'b', body: 'Hello!'});
```
