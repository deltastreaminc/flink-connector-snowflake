package io.deltastream.flink.connector.snowflake.sink.config;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SnowflakeChannelConfigTest {

    private static final String databaseName = "scct_db";
    private static final String schemaName = "scct_schema";
    private static final String tableName = "scct_table";

    @Test
    public void testChannelConfigBuild() {
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder()
                        .onErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                        .build(databaseName, schemaName, tableName);
        Assertions.assertEquals(databaseName, sfcc.getDatabaseName());
        Assertions.assertEquals(schemaName, sfcc.getSchemaName());
        Assertions.assertEquals(tableName, sfcc.getTableName());
        Assertions.assertEquals(OpenChannelRequest.OnErrorOption.CONTINUE, sfcc.getOnErrorOption());
    }

    @Test
    public void testChannelConfigBuildWithDefaultErrorOption() {
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder().build(databaseName, schemaName, tableName);
        Assertions.assertEquals(OpenChannelRequest.OnErrorOption.ABORT, sfcc.getOnErrorOption());
    }

    @Test
    public void testInvalidDatabaseName() {
        final IllegalArgumentException emptyIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeChannelConfig.builder().build("", schemaName, tableName));
        Assertions.assertEquals("Invalid database name", emptyIae.getMessage());

        final IllegalArgumentException nullIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeChannelConfig.builder().build(null, schemaName, tableName));
        Assertions.assertEquals("Invalid database name", nullIae.getMessage());
    }

    @Test
    public void testInvalidSchemaName() {
        final IllegalArgumentException emptyIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeChannelConfig.builder().build(databaseName, "", tableName));
        Assertions.assertEquals("Invalid schema name", emptyIae.getMessage());

        final IllegalArgumentException nullIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SnowflakeChannelConfig.builder()
                                        .build(databaseName, null, tableName));
        Assertions.assertEquals("Invalid schema name", nullIae.getMessage());
    }

    @Test
    public void testInvalidTableName() {
        final IllegalArgumentException emptyIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeChannelConfig.builder().build(databaseName, schemaName, ""));
        Assertions.assertEquals("Invalid table name", emptyIae.getMessage());

        final IllegalArgumentException nullIae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SnowflakeChannelConfig.builder()
                                        .build(databaseName, schemaName, null));
        Assertions.assertEquals("Invalid table name", nullIae.getMessage());
    }
}
