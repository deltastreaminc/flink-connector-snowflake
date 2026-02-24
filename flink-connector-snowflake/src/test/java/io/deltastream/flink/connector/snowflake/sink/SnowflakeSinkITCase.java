package io.deltastream.flink.connector.snowflake.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.shaded.guava33.com.google.common.collect.Maps;

import io.deltastream.flink.connector.snowflake.sink.config.ObservabilityConfig;
import io.deltastream.flink.connector.snowflake.sink.context.SnowflakeSinkContext;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.UUID;

@Testcontainers
class SnowflakeSinkITCase {

    private static final String url =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_URL", "fake.sf.com:443");
    private static final String user =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_USER", "SF_USER");
    private static final String role =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_ROLE", "SF_ROLE");
    private static final String accountId =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_ACCOUNT_ID", "myOrg-myAccountName");
    private static final String key =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_PRIVATE_KEY", "");
    private static final String keyPass =
            SystemUtils.getEnvironmentVariable("SNOWFLAKE_KEY_PASSPHRASE", "");

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testSuccessfulWriteToSnowflake() throws Exception {

        /*
         * Writes to a table with the following DDL:
         * CREATE OR REPLACE TABLE FLINK_STREAMING.PUBLIC."stream_data_tbl" (
         *      "id" VARCHAR(16777216),
         *      "data" VARCHAR(16777216)
         * );
         */

        // create a Sink with no-op write/flush to external
        final SnowflakeSinkBuilder<Map<String, Object>> sinkBuilder =
                SnowflakeSink.<Map<String, Object>>builder()
                        .url(url)
                        .user(user)
                        .role(role)
                        .observability(ObservabilityConfig.builder().enableMetrics())
                        .database("FLINK_STREAMING")
                        .schema("PUBLIC")
                        .table("\"stream_data_tbl\"") // case-sensitive table name
                        .serializationSchema(new RowPassThroughSerializer());

        // add private key, if any
        if (StringUtils.isNotBlank(key)) {
            sinkBuilder.privateKey(key);
        }

        // add private key passphrase, if any
        if (StringUtils.isNotBlank(keyPass)) {
            sinkBuilder.keyPassphrase(keyPass);
        }

        final SnowflakeSink<Map<String, Object>> sink = sinkBuilder.build("sf_sink_job");

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        Configuration.fromMap(
                                Map.of(
                                        RestartStrategyOptions.RESTART_STRATEGY.key(),
                                        RestartStrategyOptions.RestartStrategyType
                                                .NO_RESTART_STRATEGY
                                                .getMainValue())));
        env.enableCheckpointing(100L);
        env.setParallelism(1);
        env.fromSequence(1, 10).map(new SfRowMapFunction()).sinkTo(sink);
        env.execute();
    }

    private static class SfRowMapFunction implements MapFunction<Long, Map<String, Object>> {

        private static final long serialVersionUID = -2836417330784371895L;

        @Override
        public Map<String, Object> map(Long id) {
            final String uuid = UUID.randomUUID().toString();
            return Maps.newHashMap(
                    Map.of(
                            "id",
                            uuid + "-" + id,
                            "data",
                            uuid + "_" + id)); // case-sensitive column names
        }
    }

    private static class RowPassThroughSerializer
            implements SnowflakeRowSerializationSchema<Map<String, Object>> {

        private static final long serialVersionUID = -23875899103249615L;

        @Override
        public Map<String, Object> serialize(
                Map<String, Object> element, SnowflakeSinkContext sinkContext) {
            return element;
        }
    }
}
