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
* Java 17

```
git clone https://github.com/deltastreaminc/flink-connector-snowflake.git
cd flink-connector-snowflake
mvn clean package -DskipTests
```

The resulting JARs can be found in the `target` directory of the respective module.

## Maven Dependency

The Snowflake connector can be imported into a Maven project using the following dependency:

```xml
<!-- https://mvnrepository.com/artifact/io.deltastream/flink-connector-snowflake -->
<dependency>
    <groupId>io.deltastream</groupId>
    <artifactId>flink-connector-snowflake</artifactId>
    <version>${flink-connector-snowflake.version}</version>
</dependency>
```

Define a `flink-connector-snowflake.version` property in the `pom.xml` of the Maven project or replace `${flink-connector-snowflake.version}` with a version from [the Maven Central repository](https://mvnrepository.com/artifact/io.deltastream/flink-connector-snowflake).

## Using the `SnowflakeSink` API

```java
class test {

    final SnowflakeSink<Map<String, Object>> sink =
        SnowflakeSink.<Map<String, Object>>builder()
            .url("account_url")
            .user("user_name")
            .role("role_name")
            .database("DB_NAME")
            .schema("SCHEMA_NAME")
            .table("TABLE_NAME")
            .serializationSchema(SnowflakeRowSerializationSchemaImpl)
            .build("job_name_or_id");
}
```

where:

* `url` is the account URL given by Snowflake, e.g. `https://<account-id>.snowflakecomputing.com`.
* `user` is the username to use to connect to the Snowflake account.
* `role` is the role with access to the table within the Snowflake account.
* `database` is the database name within the Snowflake account. See note below about case sensitivity.
* `schema` is the schema name within the Snowflake account. See note below about case sensitivity.
* `table` is the table name within the Snowflake account. See note below about case sensitivity.
* `serializationSchema` is the serialization schema implementation of `SnowflakeRowSerializationSchema` to use to convert records to Snowflake rows.

In Snowflake, the full name of a table is case-insensitive, but UPPER_CASE biased. In other words, a sink configured with a fully qualified name of `DB.SCHEMA.table` is treated as `DB.SCHEMA.TABLE`. To be able to use case-sensitive name parts, add double quotes around them to be treated as the literal name, e.g. `DB.SCHEMA."table"`.

## Checkpointing with Flink

It's highly recommended that any checkpointing that happens using this connector to be configured to be at least 1 second to properly [optimize cost and performance](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-recommendation).

## Testing

### Required Credentials

The following credentials are used in the tests for integrating with the Snowflake service:

* `SNOWFLAKE_URL`: Account URL to use to connect to the account to write data to
* `SNOWFLAKE_USER`: Username to write data as
* `SNOWFLAKE_ROLE`: Database role to write data as
* `SNOWFLAKE_PRIVATE_KEY`: User's private key to use for connecting to the service
* `SNOWFLAKE_KEY_PASSPHRASE`: User's private key password to use for connecting to the service, as supported by OpenSSL guidelines

### Integration Test

Using the Required Credentials, test Snowflake integration with a simple write to the configured Snowflake account:

```shell
mvn clean test-compile failsafe:integration-test
```

Unit tests are run in the `test` phase:

```shell
mvn clean test
```

To debug issues with the tests, set the `rootLogger.level` in the `flink-connector-snowflake/src/test/resources/log4j2-test.properties` to `INFO` or `DEBUG`.

## IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box:

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)

Setup CheckStyle for static coding guidelines within IntelliJ IDEA:

1. Go to `Settings` → `Tools` → `Checkstyle`.
2. Set `Scan Scope` to `Only Java sources (including tests)`.
3. For `Checkstyle Version` select `10.18.2`.
4. Under `Configuration File` click the `+` icon to add a new configuration.
5. Set `Description` to `Flink`.
6. Select `Use a local Checkstyle file` and point it to tools/maven/checkstyle.xml located within your cloned repository.
7. Select `Store relative to project location` and click `Next`.
8. Configure the property checkstyle.suppressions.file with the value suppressions.xml and click `Next`.
9. Click `Finish`.
10. Select `Flink` as the only active configuration file and click `Apply`.

You can now import the Checkstyle configuration for the Java code formatter.

1. Go to `Settings` → `Editor` → `Code Style` → `Java`.
2. Click the gear icon next to `Scheme` and select `Import Scheme` → `Checkstyle Configuration`.
3. Navigate to and select tools/maven/checkstyle.xml located within your cloned repository.

To verify the setup, click `View` → `Tool Windows` → `Checkstyle` and find the `Check Module` button in the opened tool window. It should report no violations.

## Support

Don’t hesitate to ask!

[Open a Flink issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink or this connector.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your development skills.

All pull requests must be accompanied by a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Pull-request guidelines are provided when opening a new one.

## About

This connector is an open source project developed at [DeltaStream, Inc.](https://www.deltastream.io/) for use with the Apache Flink project.
