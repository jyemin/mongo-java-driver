+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrade Considerations"
[menu.main]
  identifier = "Upgrading to 3.8"
  weight = 80
  pre = "<i class='fa fa-level-up'></i>"
+++

## Upgrading from 3.8.x

Numerous classes and methods have been deprecated in the 3.9 release in preparation for a major 4.0 release, in which all deprecated
API elements except those documented as "not currently scheduled for removal" will be removed. Currently the only API elements _not_ 
scheduled for removal are:

* [`Mongo.getDB`]({{<apiref "com/mongodb/Mongo.html#getDB-java.lang.String-">}})
* [`JsonMode.STRICT`]({{<apiref "org/bson/json/JsonMode.html#STRICT">}}) 

To prepare for the 4.0 release, please compile with deprecation warnings enabled and replace all usage of deprecated API elements with their
recommended replacements.

Also, note that the 3.10 release (which will include support for MongoDB 4.2), is the last release that will be compatible with Java 6 or 
Java 7.  The 4.0 Java driver will require a minimum of Java 8. The 3.10 release will also be the last non-patch release in the 3.x line. 
In particular, support for MongoDB 4.4 will only be made available via a 4.x driver release.

The 3.9 release is binary and source compatible with the 3.8 release, except for methods that have been added to interfaces that
have been marked as unstable, and changes to classes or interfaces that have been marked as internal or annotated as Beta.

## Upgrading from 2.x

See the Upgrade guide in the 3.0 driver reference documentation for breaking changes in 3.0.

### System Requirements

The minimum JVM is now Java 6: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See
[SSL]({{< relref "driver/tutorials/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.

## Compatibility

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of MongoDB.

|Java Driver Version|MongoDB 2.6|MongoDB 3.0 |MongoDB 3.2|MongoDB 3.4|MongoDB 3.6|MongoDB 4.0|
|-------------------|-----------|------------|-----------|-----------|-----------|-----------|
|Version 3.9        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.8        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.7        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.6        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.5        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.4        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.3        |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.2        |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.1        |  ✓  |  ✓  |     |     |     |     |
|Version 3.0        |  ✓  |  ✓  |     |     |     |     |

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of Java.

|Java Driver Version| Java 6 | Java 7 | Java 8 | Java 9 |
|-------------------|--------|--------|--------|--------|
|Version 3.9        | ✓ | ✓ | ✓ | ✓ |
|Version 3.8        | ✓ | ✓ | ✓ | ✓ |
|Version 3.7        | ✓ | ✓ | ✓ | ✓ |
|Version 3.6        | ✓ | ✓ | ✓ |
|Version 3.5        | ✓ | ✓ | ✓ |
|Version 3.4        | ✓ | ✓ | ✓ |
|Version 3.3        | ✓ | ✓ | ✓ |
|Version 3.2        | ✓ | ✓ | ✓ |
|Version 3.1        | ✓ | ✓ | ✓ |
|Version 3.0        | ✓ | ✓ | ✓ |
