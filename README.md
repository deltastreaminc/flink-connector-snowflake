# Apache Flink Snowflake Connector

The repository for the Apache Flink Snowflake connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Building the Apache Flink Snowflake Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/deltastreaminc/flink-connector-snowflake.git
cd flink-connector-snowflake
mvn clean package -DskipTests
```

The resulting JARs can be found in the `target` directory of the respective module.

## Using the `SnowflakeSink` API

```java
class test {

    final SnowflakeSink<Map<String, Object>> sink =
        SnowflakeSink.<Map<String, Object>>builder()
            .url("account_url")
            .user("user_name")
            .role("role_name")
            .bufferTimeMillis(2000L)
            .database("db_name")
            .schema("schema_name")
            .table("table_name")
            .serializationSchema(SnowflakeRowSerializationSchemaImpl)
            .build("job_name_or_id");
}
```

## Required Credentials

The following credentials are used in the tests for integrating with the Snowflake service:

* `SNOWFLAKE_URL`: Account URL to use to connect to the account to write data to
* `SNOWFLAKE_USER`: Username to write data as
* `SNOWFLAKE_ROLE`: Database role to write data as
* `SNOWFLAKE_PRIVATE_KEY`: User's private key to use for connecting to the service
* `SNOWFLAKE_KEY_PASSPHRASE`: User's private key password to use for connecting to the service

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development:

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink, or file issues in this repository if you encounter bugs with this connector.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
This connector is an open source project developed at DeltaStream, Inc. for use with the Apache Flink project.
