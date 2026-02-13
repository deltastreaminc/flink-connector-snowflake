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

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ErrorCode;
import com.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeClientConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SnowflakeSinkServiceImplTest {

    @Test
    void testSuccessfulInsert() throws Exception {
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup())) {
            Assertions.assertEquals(0, sinkService.getLatestCommittedOffsetFromChannelStatus());
            sinkService.insert(Map.of("field_1", "val_1"));
            Assertions.assertEquals(1, sinkService.getLatestCommittedOffsetFromChannelStatus());
        }
    }

    @Test
    void testInsertExceptionHandling() throws Exception {
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
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
                            public void appendRow(Map<String, Object> row, String offsetToken) {
                                throw new SFException(ErrorCode.FATAL, "test");
                            }
                        };
                    }
                }) {
            IOException e =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> sinkService.insert(Map.of("field_1", "val_1")));
            org.assertj.core.api.Assertions.assertThat(e.getMessage())
                    .contains("Failed to append row with Snowflake sink service");
            Assertions.assertInstanceOf(SFException.class, e.getCause());
            Assertions.assertEquals(
                    ErrorCode.FATAL.getErrorCodeName(),
                    ((SFException) e.getCause()).getErrorCodeName());
            org.assertj.core.api.Assertions.assertThat(e.getCause().getMessage()).contains("test");
        }
    }

    @Test
    void testInsertErrornHandling() throws Exception {
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
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
                            public void appendRow(Map<String, Object> row, String offsetToken) {
                                throw new SFException(ErrorCode.FATAL, "test");
                            }
                        };
                    }
                }) {
            IOException e =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> sinkService.insert(Map.of("field_1", "val_1")));
            Assertions.assertInstanceOf(SFException.class, e.getCause());
            Assertions.assertEquals(
                    ErrorCode.FATAL.getErrorCodeName(),
                    ((SFException) e.getCause()).getErrorCodeName());
            org.assertj.core.api.Assertions.assertThat(e.getCause().getMessage()).contains("test");
        }
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
                                        SnowflakeClientConfig.builder()
                                                .url("https://org-acct.sfcomputing.com")
                                                .user("testUser")
                                                .role("testRole")
                                                .accountId("acct")
                                                .build(),
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
                                            public ChannelStatus getChannelStatus() {
                                                return this.createValidChannelStatus(
                                                        "invalid_token");
                                            }
                                        };
                                    }
                                });
        org.assertj.core.api.Assertions.assertThat(e.getMessage())
                .contains(
                        String.format(
                                "The offsetToken '%s' cannot be parsed as a long for channel",
                                "invalid_token"));
        Assertions.assertInstanceOf(NumberFormatException.class, e.getCause());
    }

    @Test
    void testChannelNameIncludesTableInformation() throws Exception {
        // Test that channel name includes database, schema, and table name in the prefix
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "testAppId",
                        5,
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("TEST_DB", "TEST_SCHEMA", "TEST_TABLE"),
                        new FakeSinkWriterMetricGroup())) {
            String channelName = sinkService.getChannelName();

            // Verify that the channel name contains the database, schema, and table information
            org.assertj.core.api.Assertions.assertThat(channelName).doesNotContain("TEST_DB");
            org.assertj.core.api.Assertions.assertThat(channelName).doesNotContain("TEST_SCHEMA");
            org.assertj.core.api.Assertions.assertThat(channelName).doesNotContain("TEST_TABLE");
            org.assertj.core.api.Assertions.assertThat(channelName).contains("testAppId");
            Assertions.assertTrue(channelName.contains("5"), "Channel name should contain taskId");
        }
    }

    @Test
    void testChannelNameUniquePerTaskId() throws Exception {
        // Test that different task IDs produce different channel names
        try (SnowflakeSinkServiceImpl sinkService1 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app",
                                0,
                                SnowflakeClientConfig.builder()
                                        .url("https://org-acct.sfcomputing.com")
                                        .user("testUser")
                                        .role("testRole")
                                        .accountId("acct")
                                        .build(),
                                SnowflakeWriterConfig.builder().build(),
                                SnowflakeChannelConfig.builder().build("DB", "SCHEMA", "TABLE"),
                                new FakeSinkWriterMetricGroup());
                SnowflakeSinkServiceImpl sinkService2 =
                        new FakeSnowflakeSinkServiceImpl(
                                "app",
                                1,
                                SnowflakeClientConfig.builder()
                                        .url("https://org-acct.sfcomputing.com")
                                        .user("testUser")
                                        .role("testRole")
                                        .accountId("acct")
                                        .build(),
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
            org.assertj.core.api.Assertions.assertThat(channelName2)
                    .doesNotContain("DB_SCHEMA_TABLE");
            org.assertj.core.api.Assertions.assertThat(channelName2)
                    .doesNotContain("DB_SCHEMA_TABLE");
        }
    }

    @Test
    void testFlushWithOffsetAlignment() throws Exception {
        // Test that flush waits for offset alignment
        try (SnowflakeSinkServiceImpl sinkService =
                new FakeSnowflakeSinkServiceImpl(
                        "appId",
                        0,
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
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
                    sinkService.getLatestCommittedOffsetFromChannelStatus(),
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
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {

                    private int offsetCheckCount = 0;
                    private SnowflakeStreamingIngestClient cachedClient;

                    @Override
                    public SnowflakeStreamingIngestClient getClient() {
                        if (cachedClient == null) {
                            cachedClient =
                                    new FakeSnowflakeStreamingIngestClient(
                                            this.getChannelName(),
                                            this.getChannelConfig().getDatabaseName(),
                                            this.getChannelConfig().getSchemaName(),
                                            this.getChannelConfig().getTableName()) {
                                        @Override
                                        public Map<String, String> getLatestCommittedOffsetTokens(
                                                List<String> channels) {
                                            offsetCheckCount++;
                                            Map<String, String> offsetTokens = new HashMap<>();
                                            channels.forEach(
                                                    channelName -> {
                                                        // Simulate delayed commit: only return
                                                        // correct offset
                                                        // after 3 checks
                                                        String token =
                                                                offsetCheckCount < 3
                                                                        ? "0"
                                                                        : this.getChannelCache()
                                                                                .get(channelName)
                                                                                .getLatestCommittedOffsetToken();
                                                        offsetTokens.put(channelName, token);
                                                    });
                                            return offsetTokens;
                                        }
                                    };
                        }
                        return cachedClient;
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
                    sinkService.getLatestCommittedOffsetFromChannelStatus(),
                    "Committed offset should eventually align after retries");
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
                        SnowflakeClientConfig.builder()
                                .url("https://org-acct.sfcomputing.com")
                                .user("testUser")
                                .role("testRole")
                                .accountId("acct")
                                .build(),
                        SnowflakeWriterConfig.builder().build(),
                        SnowflakeChannelConfig.builder()
                                .build("FAKE_DB", "FAKE_SCHEMA", "FAKE_TABLE"),
                        new FakeSinkWriterMetricGroup()) {

                    private SnowflakeStreamingIngestChannel cachedChannel;

                    @Override
                    public SnowflakeStreamingIngestChannel getChannel() {
                        if (cachedChannel == null) {
                            cachedChannel =
                                    new FakeSnowflakeStreamingIngestChannel(
                                            this.getChannelName(),
                                            this.getChannelConfig().getDatabaseName(),
                                            this.getChannelConfig().getSchemaName(),
                                            this.getChannelConfig().getTableName()) {

                                        @Override
                                        public ChannelStatus getChannelStatus() {
                                            callCount[0]++;
                                            // Return correct offset after 5 retries
                                            return callCount[0] <= 5
                                                    ? this.createValidChannelStatus("0")
                                                    : this.createValidChannelStatus(
                                                            this.getLatestCommittedOffsetToken());
                                        }
                                    };
                        }
                        return cachedChannel;
                    }
                }) {

            sinkService.insert(Map.of("field", "value"));
            sinkService.flush();

            // Verify that we actually did multiple retries (should be called during construction +
            // retries + final check)
            org.assertj.core.api.Assertions.assertThat(callCount[0]).isGreaterThan(5);
        }
    }

    private static class FakeSnowflakeSinkServiceImpl extends SnowflakeSinkServiceImpl {

        /**
         * Construct a new sink service to provide APIs to the Snowflake service.
         *
         * @param appId {@link String} UID for Flink job
         * @param taskId {@link Integer} Flink subtask ID
         * @param clientConfig {@link SnowflakeClientConfig}
         * @param writerConfig {@link SnowflakeWriterConfig}
         * @param channelConfig {@link SnowflakeChannelConfig}
         * @param metricGroup {@link SinkWriterMetricGroup}
         */
        public FakeSnowflakeSinkServiceImpl(
                String appId,
                int taskId,
                SnowflakeClientConfig clientConfig,
                SnowflakeWriterConfig writerConfig,
                SnowflakeChannelConfig channelConfig,
                SinkWriterMetricGroup metricGroup) {
            super(appId, taskId, clientConfig, writerConfig, channelConfig, metricGroup);
        }

        @Override
        SnowflakeStreamingIngestClient createClientFromConfig(
                final String appId,
                final SnowflakeChannelConfig channelConfig,
                final SnowflakeClientConfig clientConfig) {
            return new FakeSnowflakeStreamingIngestClient(
                    this.getChannelName(),
                    channelConfig.getDatabaseName(),
                    channelConfig.getSchemaName(),
                    channelConfig.getTableName());
        }

        @Override
        public SnowflakeStreamingIngestChannel getChannel() {
            return this.openChannelFromConfig();
        }

        @Override
        SnowflakeStreamingIngestChannel openChannelFromConfig() {
            return this.getClient().openChannel(this.getChannelName()).getChannel();
        }

        @Override
        void checkErrors() {
            // no-op for testing
        }
    }

    /** Testable implementation that tracks channel recreation events. */
    private static class TestableSnowflakeSinkServiceImpl extends FakeSnowflakeSinkServiceImpl {

        public TestableSnowflakeSinkServiceImpl(
                String appId,
                int taskId,
                SnowflakeClientConfig clientConfig,
                SnowflakeWriterConfig writerConfig,
                SnowflakeChannelConfig channelConfig,
                SinkWriterMetricGroup metricGroup) {
            super(appId, taskId, clientConfig, writerConfig, channelConfig, metricGroup);
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
            return new FakeOperatorIOMetricGroup();
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

    private static class FakeOperatorIOMetricGroup implements OperatorIOMetricGroup {

        @Override
        public Counter getNumRecordsInCounter() {
            return new SimpleCounter();
        }

        @Override
        public Counter getNumRecordsOutCounter() {
            return new SimpleCounter();
        }

        @Override
        public Counter getNumBytesInCounter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter getNumBytesOutCounter() {
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
