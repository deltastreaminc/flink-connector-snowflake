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
        Assertions.assertTrue(e.getCause() instanceof NumberFormatException);
    }

    private class FakeSnowflakeSinkServiceImpl extends SnowflakeSinkServiceImpl {

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

    private class FakeSinkWriterMetricGroup implements SinkWriterMetricGroup {

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
