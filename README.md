# Pennsieve Streaming API

## Overview
This project provides a streaming API for pennsieve time series data.
We query an timeseries database - indexed by ranges in postgres.
The actual data is stored on S3 as gzipped binary files.
The results are resampled and streamed over a websocket.

![timeseries](https://cloud.githubusercontent.com/assets/147873/22992849/ef122b86-f38e-11e6-8135-58c35060c637.png)

## Building

To generate a JAR file suitable for deployment to a Jetty container:

```bash
sbt clean assembly
```

To fire up an embedded jetty container for testing purposes on port 8080:

```bash
sbt -Dconfig.resource=local.conf run
```

To connect to the websocket for testing, try using [Dark Websocket Terminal](https://chrome.google.com/webstore/detail/dark-websocket-terminal/dmogdjmcpfaibncngoolgljgocdabhke)


Here's a sample request you can try over the websocket:

```json
{"session":"","channels":["aba54dd6f730488a8162bc61d17ad1d1","98e6f12b91234956a0213d6f51ecd161","82a08f32ea9946718a3b1db7ee69e624","90d95eb5b9fc4c45be75e79419e52b6d","be1c07ac9b1248038ba738a2837db71e","4209e5a0d4504197a034babdf741709a","e35aa68a19464dfa90f134bd9823f07d","96b4a8ef5cd44921b2e865977616e0f1","af82a0925f0841ba812b7cc1f13bb70d","ee53b3534e024a8e92ca9717fd247584","145a9e4e52ef454c86d82d6b5d514f8b","6e37f39710ec451b9398d2d3063f5137","25400d8db6c24f979dce1903ad6b58b4","a87c75480b0340609b75894a8d01b41b","d883e6595e8046b49adedf64e06f0acb","cef8a2388f2c453084231019d4cce3e7","a3319410e21b47aebfb09244fe8cd5d8","a7086e5450c5420d9176a70241ac225d","9b09cb4744bd43518fba28bd20ec34ab","a71b3ba93cc7440f9d964157ab06b31c","5f39c3d5632b4e06b9e4e80d00fb085a","b248c989ce9645e7886b9b26ba9c9fb3","3049ab5761a7496b86d1300b82317479"],"minMax":true,"startTime":3284198937000000,"endTime":3284198942000000,"packageId":"N:fo:2cb888da-84ef-4858-8419-092338bd48aa","pixelWidth":14098}
```

to write annotations, perform an HTTP post to https://streaming.dev.pennsieve.io/timespan?session=abc123 :
```json
{"items":[{"time":123,"duration":123,"value":"SPIKE", "source":"abc"}]}
```

## Profiling

In order to performance profile the service, use the timeseries-profile startup script instead of the regular one.
This will start the server up with [JMX](https://docs.oracle.com/javase/tutorial/jmx/overview/index.html) enabled.
Once this is done, you can open an SSH tunnel for port 9090 (which is configured in the script) and use [JVisualVM](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jvisualvm.html)
to perform profiling.

```
ssh streaming.dev.pennsieve.io
sudo /etc/init.d/timeseries stop
sudo /etc/init.d/timeseries-profile start
exit #return to local machine
ssh -L 9090:localhost:9090 streaming.dev.pennsieve.io #on the local machine, port forward 9090 to the streaming server port 9090
jvisualvm # on your local machine
```

Once you've opened the JVisualVM app, open a new JMX connection to localhost:9090, and start profiling!
