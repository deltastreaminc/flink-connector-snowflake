package io.deltastream.flink.connector.snowflake.sink.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SnowflakeChannelConfigTest {

    private static final String databaseName = "scct_db";
    private static final String schemaName = "scct_schema";
    private static final String tableName = "scct_table";

    @Test
    public void testChannelConfigBuild() {
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder().build(databaseName, schemaName, tableName);
        Assertions.assertEquals(databaseName, sfcc.getDatabaseName());
        Assertions.assertEquals(schemaName, sfcc.getSchemaName());
        Assertions.assertEquals(tableName, sfcc.getTableName());
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

    @Test
    public void testDefaultChannelCloseTimeout() {
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder().build(databaseName, schemaName, tableName);
        Assertions.assertEquals(
                SnowflakeChannelConfig.CHANNEL_CLOSE_TIMEOUT_MS_DEFAULT,
                sfcc.getChannelCloseTimeout().toMillis());
    }

    @Test
    public void testCustomChannelCloseTimeout() {
        final long customTimeoutMs = 15_000L; // 15 seconds
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder()
                        .channelCloseTimeoutMs(customTimeoutMs)
                        .build(databaseName, schemaName, tableName);
        Assertions.assertEquals(customTimeoutMs, sfcc.getChannelCloseTimeout().toMillis());
    }

    @Test
    public void testNegativeChannelCloseTimeout() {
        final IllegalArgumentException iae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeChannelConfig.builder().channelCloseTimeoutMs(-1));
        Assertions.assertEquals("channelCloseTimeoutMs must be non-negative", iae.getMessage());
    }

    @Test
    public void testZeroChannelCloseTimeout() {
        final SnowflakeChannelConfig sfcc =
                SnowflakeChannelConfig.builder()
                        .channelCloseTimeoutMs(0L)
                        .build(databaseName, schemaName, tableName);
        Assertions.assertEquals(Integer.MAX_VALUE, sfcc.getChannelCloseTimeout().toMillis());
    }
}
