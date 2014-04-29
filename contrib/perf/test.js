#!/usr/bin/env node

var async = require('async');
var optimist = require('optimist');
var StatsD = require('node-statsd').StatsD;
var Identity = require('pkgcloud/lib/pkgcloud/rackspace/identity').Identity;
var http = require('https');
var util = require('util');
var successes = 0,
    requests = 0,
    failures = 0,
    rateStore = [],
    keepAliveAgent, argv, reqOpts, reqObj, identityClient, token, client;

var MAX_RETRY_COUNT = 1,
    IDENTITY_SERVICE_URL = 'https://staging.identity.api.rackspacecloud.com';

var argparsing = optimist
  .usage('\nBenchmark Blueflood ingestion of metrics.\n\nUsage $0 {options}').wrap(150)
  .options('x', {
    'alias': 'tenantId',
    'desc': 'TenantID to be used with the metrics',
    'default': '123456'
  })
  //.string('id')
  .options('n', {
    'alias': 'metrics',
    'desc': 'Number of metrics per batch.',
    'default': 1
  })
  .options('i', {
    'alias': 'interval',
    'desc': 'Interval in milliseconds between the reported collected_at time on data points being produced',
    'default': 30000
  })
  .options('d', {
    'alias': 'duration',
    'desc': 'How many minutes ago the first datapoint will be reported as having been collected at.',
    'default': 60
  })
  .options('b', {
    'alias': 'batches',
    'desc': 'Number of batches to send',
    'default': 20
  })
  .options('c', {
    'alias': 'chunked',
    'desc': 'Whether to use chunked encoding',
    'default': false
  })
  .options('r', {
    'alias': 'reports',
    'desc': 'Maximum number of reporting intervals (each 10s), then stop the benchmark',
    'default': 0
  })
  .options('v', {
    'alias': 'reportingInterval',
    'desc': 'Reporting interval',
    'default': 5000
  })
  .options('statsd', {
    'desc': 'Whether to report to statsd. Defaults to reporting to a local statsd on default port',
    'default': true
  })
  .options('m', {
    'alias': 'multitenant',
    'desc': 'Multi-tenant mode',
    'default': false
  })
  .options('e', {
    'alias': 'ingestionEndpoint',
    'desc': 'Endpoint for ingestion',
    'default': 'localhost'
  })
  .options('p', {
    'alias': 'ingestionPort',
    'desc': 'Ingestion port',
    'default': 19000
  })
  .options('t', {
    'alias': 'tenants',
    'desc': 'Tenants to be used for multitenant submission',
    'default': ['967453', '238402']
  })
  .options('e', {
    'alias': 'errorTolerance',
    'desc': 'Maximum number of errors to be tolerated',
    'default': 10
  })
  .options('a', {
    'alias': 'doAuthentication',
    'desc': 'Whether to do authentication',
    'default': false
  })
  .options('u', {
    'alias': 'authCreds',
    'desc': 'Authentication Creds to be used',
    'default': {'username':'foo', 'apiKey':'bar'}
  });


function makeRequest(metrics, retryCount, callback) {
  var metricsString = JSON.stringify(metrics),
      startTime = new Date().getTime(),
      req = reqObj.request(reqOpts, function(res) {
        if (argv.statsd) {
          client.timing('request_time', new Date().getTime() - startTime);
        }

        if (res.statusCode === 200) {
          successes++;
        } else if (res.statusCode === 401 && retryCount <= MAX_RETRY_COUNT) {
          // get a new token and retry
          _getToken (function(err) {
            if (err) {
              finalReportStatus(err);
            }
            reqOpts.headers['x-auth-token'] = token;
          });
          makeRequest(metrics, retryCount++, callback);
        } else {
          res.setEncoding('utf8');
          res.on('data', function (chunk) {
              if (argv.e <= failures++) {
                var error = new Error("Shutting down the benchmark because tolerance for errors has been exceedeed\n");
                callback(error); // causes the error to be bubbled up and finally stops the benchmarking
              }
          });
        }
        res.resume(); // makes it so that we can re-use the connection without having to read the response body
        callback();
      });

  if (!argv.c) {
    req.setHeader('Content-Length', metricsString.length);
  }

  if (metricsString.length > 1048576) {
    console.warn('Exceeding maximum length of 1048576, attempted to send ' + metricsString.length + ' -- Blueflood probably is failing your requests!!');
  }

  req.on('error', function(err) {
    callback(err);
  });

  req.write(metricsString);
  req.end();
  requests++;
};


// Send argv.n metrics as a batch many times
function sendMetricsForBatch(batchPrefix, callback) {
    // Blueflood understands millis since epoch only
    // Publish metrics with older timestamps (argv.duration minutes before start time)
    var startTime = new Date().getTime(),
        sendTimestamp = startTime - (argv.duration * 1000 * 60),
        j, metrics;

    async.until(
      function done() {
        return sendTimestamp >= startTime;
      },
      function sendOneBatch(callback) {
        metrics = [];
        for (j = 0; j < argv.n; j++) {
          var metric = {};
          metric['collectionTime'] = sendTimestamp;
          metric['metricName'] = batchPrefix + j;
          metric['metricValue'] = Math.random() * 100;
          metric['ttlInSeconds'] = 172800; //(2 * 24 * 60 * 60) //  # 2 days
          metric['unit'] = 'seconds';
          if (argv.multitenant) {
            metric['tenantId'] = argv.tenants[_randomIntInc(0, argv.tenants.length)] // selects a random tenant for stamping on each metric
          }
          metrics.push(metric);
        }
        sendTimestamp += argv.i;
        makeRequest(metrics, 0, callback);
      },
      function(err) {
        callback(err);
      });
}


function _randomIntInc(low, high) {
    return Math.floor(Math.random() * (high - low + 1) + low);
}


function _getToken(callback) {
  identityClient.authorize({'url':IDENTITY_SERVICE_URL}, function(err) {
    if (err) {
      callback(err);
    }
    token = identityClient.token.id;
    callback();
  });
}


function _getMean(numbers) {
  var sum = 0;
  for (var i = 0; i < numbers.length; i++) {
    sum += numbers[i];
  }
  return (sum / numbers.length);
}


function _getStdDeviation(numbers) {
  var distance = 0,
      mean = _getMean(numbers);
  for (var i = 0; i < numbers.length; i++) {
    distance += Math.pow((numbers[i] - mean), 2);
  }
  return Math.sqrt((distance/numbers.length));
}


// Send many batches
function sendBatches() {
  var batchPrefixes = [];
  for (var i = 0; i < argv.batches; i++) {
    batchPrefixes.push(i.toString() + '.');
  }
  async.map(batchPrefixes,
            sendMetricsForBatch,
            function(err) {
              finalReportStatus(err);
            });
}


function setupReporting() {
  var startTime = new Date().getTime(),
      lastSuccessCount = 0,
      successWithinInterval, timeTaken;
        
  function reportStatus() {
    timeTaken = new Date().getTime() - startTime;
    successWithinInterval = successes - lastSuccessCount;
    rateWithinInterval = (successWithinInterval * argv.n / (argv.v / 1000.0));

    if (!isNaN(rateWithinInterval)) {
      rateStore.push(rateWithinInterval)
    }

    console.log(util.format('%d \t %d \t %d \t %d \t %d \t %d \n',
               (rateWithinInterval).toFixed(0),
               (successWithinInterval / (argv.v / 1000.0)).toFixed(0),
               requests, successes, failures, timeTaken));

    lastSuccessCount = successes;
    if (argv.r && (timeTaken >= (argv.r * 10000))) {
      finalReportStatus(null);
    }
  };

  console.log('Points\tMetrics\tBatches\tM/Batch\tInterv\tDur\tPoints/metric');
  console.log(util.format('%d\t%d\t%d\t%d\t%d\t%d\t%d\n', argv.b * argv.n, argv.n, argv.b, argv.n, argv.i, argv.d, (argv.d * 60000.0 / argv.i).toFixed(0)));
  console.log('M/s\tReq/s\tTotal\t2xx\tErrors\t Time\n');

  setInterval(reportStatus, argv.v);
}


function finalReportStatus(err) {
      console.log('Total Metrics Sent \t Total Request Made \t Total Successes \t Total Errors \t  Max rate \t Min rate \t Average rate \t Standard Deviation');
      console.log(util.format('\t%d\t\t\t%d\t\t\t%d\t\t\t%d\t\t%d\t\t%d\t\t%d\t\t%d\n\n', requests * argv.n, requests, successes, failures, Math.max.apply(null, rateStore), Math.min.apply(null, rateStore), _getMean(rateStore), _getStdDeviation(rateStore)));
      if (err) {
         console.log('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Benchmarking encountered error~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n');
         console.error('Error encountered : %s', err.message)
         process.exit(1);
      } else {
        console.log('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Benchmarking Done~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n');
        process.exit(0);
      }
}


function startup() {
  argv = argparsing.argv;
  if (argv.help) {
    argparsing.showHelp(console.log);
    console.log("M/s -- All time metrics per second.");
    console.log('Req/s -- Requests per second');
    console.log('Total -- Total requests made (includes in-progress reqs)');
    console.log('2xx -- Successful requests (only includes completed reqs)');
    console.log('Errors -- Errors encountered')
    console.log('Time -- Total time since starting the script, in milliseconds');
    process.exit(0);
  }

  if (argv.statsd) {
    client = new StatsD();
  }

  reqOpts = {
    host: argv.ingestionEndpoint,
    //port: argv.ingestionPort,
    path: argv.multitenant ? ('/v1.0/multitenant/experimental/metrics') : ('/v1.0/' + argv.tenantId + '/experimental/metrics'),
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Connection': 'keep-alive' 
    }
  };

  keepAliveAgent = new http.Agent({ keepAlive: true, maxSockets: argv.b });

  // Node version compatibility thing
  if (typeof(keepAliveAgent.request) === 'function') {
    reqObj = keepAliveAgent;
  } else {
    reqOpts.agent = keepAliveAgent;
    reqObj = http;
  }

  if (argv.doAuthentication) {
    identityClient = new Identity(JSON.parse(argv.u));
    _getToken (function(err) {
      if (err) {
        finalReportStatus(err);
      }
      reqOpts.headers['x-auth-token'] = token;
      setupReporting();
      sendBatches();
    });
  } else {
    setupReporting();
    sendBatches();
  }
}

startup()

