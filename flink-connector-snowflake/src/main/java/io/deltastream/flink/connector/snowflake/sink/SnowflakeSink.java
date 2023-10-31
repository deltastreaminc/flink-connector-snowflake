/*
 * Copyright (c) 2023 DeltaStream, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.deltastream.flink.connector.snowflake.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import io.deltastream.flink.connector.snowflake.sink.context.DefaultSnowflakeSinkContext;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;

import java.util.Properties;

/**
 * Flink Sink to produce data into a Snowflake table. The sink supports below delivery guarantees as
 * described by {@link org.apache.flink.connector.base.DeliveryGuarantee}.
 *
 * <ul>
 *   {@link org.apache.flink.connector.base.DeliveryGuarantee#NONE} does not provide any guarantees:
 *   messages may be lost in case of issues on the Snowflake ingest channel and messages may be
 *   duplicated in case of a Flink runtime failure.
 * </ul>
 *
 * <ul>
 *   {@link org.apache.flink.connector.base.DeliveryGuarantee#AT_LEAST_ONCE} the sink will flush
 *   data on a checkpoint to ensure all received events have successfully been committed to the
 *   Snowflake service backend. Ingestion failures are retried to ensure delivery of all received
 *   events at least once, but data may be duplicated when Flink restarts.
 * </ul>
 *
 * @param <IN> type of records that the sink receives to serialize and write to the corresponding
 *     Snowflake table
 * @see SnowflakeSinkBuilder for constructing this sink
 */
public class SnowflakeSink<IN> implements Sink<IN> {

    private static final long serialVersionUID = 3587917829427569404L;

    private final String appId;
    private final Properties connectionConfigs;
    private final SnowflakeWriterConfig writerConfig;
    private final SnowflakeChannelConfig channelConfig;
    private final SnowflakeRowSerializationSchema<IN> serializationSchema;

    public String getAppId() {
        return appId;
    }

    public Properties getConnectionConfigs() {
        return connectionConfigs;
    }

    public SnowflakeWriterConfig getWriterConfig() {
        return writerConfig;
    }

    public SnowflakeChannelConfig getChannelConfig() {
        return channelConfig;
    }

    public SnowflakeRowSerializationSchema<IN> getSerializationSchema() {
        return serializationSchema;
    }

    SnowflakeSink(
            final String appId,
            final Properties connectionConfigs,
            final SnowflakeWriterConfig writerConfig,
            final SnowflakeChannelConfig channelConfig,
            final SnowflakeRowSerializationSchema<IN> serializationSchema) {

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(appId), "An application ID is required");
        this.appId = appId;
        this.connectionConfigs = Preconditions.checkNotNull(connectionConfigs);
        this.writerConfig = Preconditions.checkNotNull(writerConfig);
        this.channelConfig = Preconditions.checkNotNull(channelConfig);
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
    }

    public static <IN> SnowflakeSinkBuilder<IN> builder() {
        return new SnowflakeSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) {
        return new SnowflakeSinkWriter<>(
                new DefaultSnowflakeSinkContext(
                        Preconditions.checkNotNull(initContext, "initContext"),
                        this.writerConfig,
                        this.appId),
                this.connectionConfigs,
                this.channelConfig,
                this.serializationSchema);
    }
}
