package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import net.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.InsertValidationResponse.InsertError;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class SnowflakeSinkServiceImplTest {

    @Test
    void testSuccessfulInsert() throws IOException {
        SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup());
        Assertions.assertEquals(
                0, sinkService.getLatestCommittedOffsetFromSnowflakeIngestChannel());
        sinkService.insert(Map.of("field_1", "val_1"));
        Assertions.assertEquals(
                1, sinkService.getLatestCommittedOffsetFromSnowflakeIngestChannel());
    }

    @Test
    void testInsertExceptionHandling() throws IOException {
        SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {
                    @Override
                    public SnowflakeStreamingIngestChannel getChannel() {
                        return new FakeSnowflakeStreamingIngestChannel(
                                this.getChannelName(),
                                this.getChannelConfig().getDatabaseName(),
                                this.getChannelConfig().getSchemaName(),
                                this.getChannelConfig().getTableName()) {
                            @Override
                            public InsertValidationResponse insertRow(
                                    Map<String, Object> row, String offsetToken) {
                                throw new SFException(ErrorCode.INTERNAL_ERROR, "test");
                            }
                        };
                    }
                };
        IOException e =
                Assertions.assertThrows(
                        IOException.class, () -> sinkService.insert(Map.of("field_1", "val_1")));
        Assertions.assertTrue(
                e.getMessage().contains("Failed to insert row with Snowflake sink service"));
    }

    @Test
    void testInsertErrornHandling() throws IOException {
        SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {
                    @Override
                    public SnowflakeStreamingIngestChannel getChannel() {
                        return new FakeSnowflakeStreamingIngestChannel(
                                this.getChannelName(),
                                this.getChannelConfig().getDatabaseName(),
                                this.getChannelConfig().getSchemaName(),
                                this.getChannelConfig().getTableName()) {
                            @Override
                            public InsertValidationResponse insertRow(
                                    Map<String, Object> row, String offsetToken) {
                                InsertValidationResponse res = new InsertValidationResponse();
                                InsertError insertError =
                                        new InsertError(row, Long.parseLong(offsetToken));
                                insertError.setException(
                                        new SFException(ErrorCode.INTERNAL_ERROR, "test"));
                                res.addError(insertError);
                                return res;
                            }
                        };
                    }
                };
        IOException e =
                Assertions.assertThrows(
                        IOException.class, () -> sinkService.insert(Map.of("field_1", "val_1")));
        Assertions.assertTrue(
                e.getMessage()
                        .contains(
                                "Encountered errors while ingesting rows into Snowflake: Ingest client internal error: test."));
    }

    @Test
    void testFetchOffsetTokenErrorHandling() {
        @SuppressWarnings("resource")
        FlinkRuntimeException e =
                Assertions.assertThrows(
                        FlinkRuntimeException.class,
                        () ->
                                new FakeSnowflakeSinkServiceImpl(
                                        "appId",
                                        0,
                                        new Properties(),
                                        SnowflakeWriterConfig.builder().build(),
                                        SnowflakeChannelConfig.builder()
                                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                                        new FakeSinkWriterMetricGroup()) {
                                    @Override
                                    public SnowflakeStreamingIngestClient getClient() {
                                        return new FakeSnowflakeStreamingIngestClient(
                                                this.getChannelName()) {
                                            @Override
                                            public Map<String, String>
                                                    getLatestCommittedOffsetTokens(
                                                            List<SnowflakeStreamingIngestChannel>
                                                                    channels) {
                                                Map<String, String> offsetTokens = new HashMap<>();
                                                channels.forEach(
                                                        c -> {
                                                            String fqn = c.getFullyQualifiedName();
                                                            String token = "invalid_token";
                                                            offsetTokens.put(fqn, token);
                                                        });
                                                return offsetTokens;
                                            }
                                        };
                                    }
                                });
        Assertions.assertTrue(
                e.getMessage()
                        .contains(
                                String.format(
                                        "The offsetToken '%s' cannot be parsed as a long for channel",
                                        "invalid_token")));
        Assertions.assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testChannelNameIncludesTableInformation() {
        // Test that channel name includes database, schema, and table name in the prefix
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "testAppId",
                        5,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("TEST_DB", "TEST_SCHEMA", "TEST_TABLE"),
                        new FakeSinkWriterMetricGroup())) {
            String channelName = sinkService.getChannelName();

            // Verify that the channel name contains the database, schema, and table information
            Assertions.assertTrue(
                    channelName.contains("TEST_DB"), "Channel name should contain database name");
            Assertions.assertTrue(
                    channelName.contains("TEST_SCHEMA"), "Channel name should contain schema name");
            Assertions.assertTrue(
                    channelName.contains("TEST_TABLE"), "Channel name should contain table name");
            Assertions.assertTrue(
                    channelName.contains("testAppId"), "Channel name should contain appId");
            Assertions.assertTrue(channelName.contains("5"), "Channel name should contain taskId");
        } catch (Exception e) {
            Assertions.fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    @Test
    void testChannelNameFormatWithDifferentConfigs() {
        // Test channel name format with different database/schema/table combinations
        try (SnowflakeSinkServiceImpl sinkService1 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app1",
                                0,
                                new Properties(),
                                SnowflakeWriterConfig.builder().build(),
                                SnowflakeChannelConfig.builder().build("DB1", "SCHEMA1", "TABLE1"),
                                new FakeSinkWriterMetricGroup());
                SnowflakeSinkServiceImpl sinkService2 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app1",
                                0,
                                new Properties(),
                                SnowflakeWriterConfig.builder().build(),
                                SnowflakeChannelConfig.builder().build("DB2", "SCHEMA2", "TABLE2"),
                                new FakeSinkWriterMetricGroup())) {

            String channelName1 = sinkService1.getChannelName();
            String channelName2 = sinkService2.getChannelName();

            // Verify that different table configurations produce different channel names
            Assertions.assertNotEquals(
                    channelName1,
                    channelName2,
                    "Different table configurations should produce different channel names");

            // Verify the expected prefix pattern for each
            Assertions.assertTrue(
                    channelName1.startsWith("DB1_SCHEMA1_TABLE1"),
                    "Channel name should start with DB_SCHEMA_TABLE prefix");
            Assertions.assertTrue(
                    channelName2.startsWith("DB2_SCHEMA2_TABLE2"),
                    "Channel name should start with DB_SCHEMA_TABLE prefix");
        } catch (Exception e) {
            Assertions.fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    @Test
    void testChannelNameUniquePerTaskId() {
        // Test that different task IDs produce different channel names
        try (SnowflakeSinkServiceImpl sinkService1 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app",
                                0,
                                new Properties(),
                                SnowflakeWriterConfig.builder().build(),
                                SnowflakeChannelConfig.builder().build("DB", "SCHEMA", "TABLE"),
                                new FakeSinkWriterMetricGroup());
                SnowflakeSinkServiceImpl sinkService2 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app",
                                1,
                                new Properties(),
                                SnowflakeWriterConfig.builder().build(),
                                SnowflakeChannelConfig.builder().build("DB", "SCHEMA", "TABLE"),
                                new FakeSinkWriterMetricGroup())) {

            String channelName1 = sinkService1.getChannelName();
            String channelName2 = sinkService2.getChannelName();

            // Verify that different task IDs produce different channel names
            Assertions.assertNotEquals(
                    channelName1,
                    channelName2,
                    "Different task IDs should produce different channel names");

            // Both should contain the same table prefix
            Assertions.assertTrue(channelName1.contains("DB_SCHEMA_TABLE"));
            Assertions.assertTrue(channelName2.contains("DB_SCHEMA_TABLE"));
        } catch (Exception e) {
            Assertions.fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    @Test
    void testFlushWithOffsetAlignment() throws Exception {
        // Test that flush waits for offset alignment
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup())) {

            // Insert some records
            sinkService.insert(Map.of("field_1", "val_1"));
            sinkService.insert(Map.of("field_2", "val_2"));
            sinkService.insert(Map.of("field_3", "val_3"));

            // Flush should wait for committed offset to align
            sinkService.flush();

            // Verify offset is aligned after flush
            Assertions.assertEquals(
                    3,
                    sinkService.getLatestCommittedOffsetFromSnowflakeIngestChannel(),
                    "Committed offset should match inserted records after flush");
        }
    }

    @Test
    void testFlushWithDelayedOffsetCommit() throws Exception {
        // Test that flush retries when offset commit is delayed
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {

                    private int offsetCheckCount = 0;

                    @Override
                    public SnowflakeStreamingIngestClient getClient() {
                        return new FakeSnowflakeStreamingIngestClient(this.getChannelName()) {
                            @Override
                            public Map<String, String> getLatestCommittedOffsetTokens(
                                    List<SnowflakeStreamingIngestChannel> channels) {
                                offsetCheckCount++;
                                Map<String, String> offsetTokens = new HashMap<>();
                                channels.forEach(
                                        c -> {
                                            String fqn = c.getFullyQualifiedName();
                                            // Simulate delayed commit: only return correct offset
                                            // after 3 checks
                                            String token =
                                                    offsetCheckCount < 3
                                                            ? "0"
                                                            : c.getLatestCommittedOffsetToken();
                                            offsetTokens.put(fqn, token);
                                        });
                                return offsetTokens;
                            }
                        };
                    }
                }) {

            // Insert records
            sinkService.insert(Map.of("field_1", "val_1"));
            sinkService.insert(Map.of("field_2", "val_2"));

            // Flush should retry and eventually succeed
            sinkService.flush();

            // Verify offset eventually aligned
            Assertions.assertEquals(
                    2,
                    sinkService.getLatestCommittedOffsetFromSnowflakeIngestChannel(),
                    "Committed offset should eventually align after retries");
        }
    }

    @Test
    void testChannelRecreationAfterThreshold() throws Exception {
        // Test that channel is recreated after 1M inserts
        final int threshold = 1_000_000;

        try (TestableSnowflakeSinkServiceImpl sinkService =
                new TestableSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup())) {

            // Insert threshold number of records
            for (int i = 0; i < threshold; i++) {
                sinkService.insert(Map.of("field", "value_" + i));
            }

            // Flush should trigger channel recreation
            sinkService.flush();

            // Verify channel was recreated by checking the flag
            Assertions.assertTrue(
                    sinkService.wasChannelRecreated(),
                    "Channel should be recreated after reaching threshold");
        }
    }

    @Test
    void testChannelNotRecreatedBelowThreshold() throws Exception {
        // Test that channel is NOT recreated below 1M inserts
        try (TestableSnowflakeSinkServiceImpl sinkService =
                new TestableSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup())) {

            // Insert well below threshold
            for (int i = 0; i < 100; i++) {
                sinkService.insert(Map.of("field", "value_" + i));
            }

            // Flush should NOT trigger channel recreation
            sinkService.flush();

            // Verify channel was not recreated
            Assertions.assertFalse(
                    sinkService.wasChannelRecreated(),
                    "Channel should not be recreated below threshold");
        }
    }

    @Test
    void testRetryCountLogging() throws Exception {
        // Test that retry attempts are properly tracked
        final int[] callCount = {0};

        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        new Properties(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {

                    @Override
                    public SnowflakeStreamingIngestClient getClient() {
                        return new FakeSnowflakeStreamingIngestClient(this.getChannelName()) {
                            @Override
                            public Map<String, String> getLatestCommittedOffsetTokens(
                                    List<SnowflakeStreamingIngestChannel> channels) {
                                callCount[0]++;
                                Map<String, String> offsetTokens = new HashMap<>();
                                channels.forEach(
                                        c -> {
                                            String fqn = c.getFullyQualifiedName();
                                            // Return correct offset after 5 retries
                                            String token =
                                                    callCount[0] <= 5
                                                            ? "0"
                                                            : c.getLatestCommittedOffsetToken();
                                            offsetTokens.put(fqn, token);
                                        });
                                return offsetTokens;
                            }
                        };
                    }
                }) {

            sinkService.insert(Map.of("field", "value"));
            sinkService.flush();

            // Verify that we actually did multiple retries (should be called during construction +
            // retries + final check)
            Assertions.assertTrue(callCount[0] > 5, "Should have made multiple retry attempts");
        }
    }

    private static class FakeSnowflakeSinkServiceImpl extends SnowflakeSinkServiceImpl {

        /**
         * Construct a new sink service to provide APIs to the Snowflake service.
         *
         * @param appId {@link String} UID for Flink job
         * @param taskId {@link Integer} Flink subtask ID
         * @param connectionConfig {@link Properties} Snowflake connection settings
         * @param writerConfig {@link SnowflakeWriterConfig}
         * @param channelConfig {@link SnowflakeChannelConfig}
         * @param metricGroup {@link SinkWriterMetricGroup}
         */
        public FakeSnowflakeSinkServiceImpl(
                String appId,
                int taskId,
                Properties connectionConfig,
                SnowflakeWriterConfig writerConfig,
                SnowflakeChannelConfig channelConfig,
                SinkWriterMetricGroup metricGroup) {
            super(appId, taskId, connectionConfig, writerConfig, channelConfig, metricGroup);
        }

        @Override
        SnowflakeStreamingIngestClient createClientFromConfig(
                final String appId, final Properties connectionConfig) {
            return new FakeSnowflakeStreamingIngestClient(this.getChannelName());
        }
    }

    /** Testable implementation that tracks channel recreation events. */
    private static class TestableSnowflakeSinkServiceImpl extends FakeSnowflakeSinkServiceImpl {
        private boolean channelRecreated = false;

        public TestableSnowflakeSinkServiceImpl(
                String appId,
                int taskId,
                Properties connectionConfig,
                SnowflakeWriterConfig writerConfig,
                SnowflakeChannelConfig channelConfig,
                SinkWriterMetricGroup metricGroup) {
            super(appId, taskId, connectionConfig, writerConfig, channelConfig, metricGroup);
        }

        @Override
        void recreateChannel() {
            super.recreateChannel();
            channelRecreated = true;
        }

        public boolean wasChannelRecreated() {
            return channelRecreated;
        }
    }

    private static class FakeSinkWriterMetricGroup implements SinkWriterMetricGroup {

        @Override
        public Counter getNumRecordsOutErrorsCounter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter getNumRecordsSendErrorsCounter() {
            return new SimpleCounter();
        }

        @Override
        public Counter getNumRecordsSendCounter() {
            return new SimpleCounter();
        }

        @Override
        public Counter getNumBytesSendCounter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter counter(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MetricGroup addGroup(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getScopeComponents() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getAllVariables() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            throw new UnsupportedOperationException();
        }
    }
}
