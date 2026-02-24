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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import io.deltastream.flink.connector.snowflake.sink.config.ObservabilityConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeClientConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;

/**
 * Builder for constructing a {@link SnowflakeSink} with appropriate configurations.
 *
 * @param <IN> type of the events produced by Flink
 */
@PublicEvolving
public class SnowflakeSinkBuilder<IN> {

    private final SnowflakeClientConfig.SnowflakeClientConfigBuilder clientConfigBuilder =
            SnowflakeClientConfig.builder();
    private final SnowflakeWriterConfig.SnowflakeWriterConfigBuilder writerConfigBuilder =
            SnowflakeWriterConfig.builder();
    private final SnowflakeChannelConfig.SnowflakeChannelConfigBuilder channelConfigBuilder =
            SnowflakeChannelConfig.builder();
    private String database;
    private String schema;
    private String table;
    private SnowflakeRowSerializationSchema<IN> serializationSchema;

    // ====================================================================
    // Client and connection configuration
    // ====================================================================

    /**
     * Set the connection URL for connecting to the Snowflake service.
     *
     * @param connectionUrl {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> url(final String connectionUrl) {
        this.clientConfigBuilder.url(connectionUrl);
        return this;
    }

    /**
     * Set the user connecting to the Snowflake service.
     *
     * @param connectionUser {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> user(final String connectionUser) {
        this.clientConfigBuilder.user(connectionUser);
        return this;
    }

    /**
     * Set the role as to connect to the Snowflake service.
     *
     * @param connectionRole {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> role(final String connectionRole) {
        this.clientConfigBuilder.role(connectionRole);
        return this;
    }

    /**
     * Set the private key to connect with to the Snowflake service. The private key must only
     * include the key content without any header, footer, or newline feeds.
     *
     * @param connectionPrivateKey {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> privateKey(final String connectionPrivateKey) {
        this.clientConfigBuilder.privateKey(connectionPrivateKey);
        return this;
    }

    /**
     * Set the private key password for the private key in {@link #privateKey(String)}.
     *
     * @param connectionKeyPassphrase {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> keyPassphrase(final String connectionKeyPassphrase) {
        this.clientConfigBuilder.keyPassphrase(connectionKeyPassphrase);
        return this;
    }

    /**
     * Set the Snowflake account identifier. This is optional if the URL provided via {@link
     * #url(String)} follows the format {@code https://<account_id>.snowflakecomputing.com}, as the
     * account ID will be automatically extracted from the URL. If provided explicitly, this value
     * takes precedence over the extracted value.
     *
     * @param account {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> accountId(final String account) {
        this.clientConfigBuilder.accountId(account);
        return this;
    }

    // ====================================================================
    // Channel configuration
    // ====================================================================

    /**
     * Set the database name to sink to in Snowflake.
     *
     * @param database {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> database(final String database) {
        this.database = Preconditions.checkNotNull(database);
        return this;
    }

    /**
     * Set the schema name to sink to in Snowflake.
     *
     * @param schema {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> schema(final String schema) {
        this.schema = Preconditions.checkNotNull(schema);
        return this;
    }

    /**
     * Set the table name to sink to in Snowflake.
     *
     * @param table {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> table(final String table) {
        this.table = Preconditions.checkNotNull(table);
        return this;
    }

    // ====================================================================
    // Sink writer configuration
    // ====================================================================

    /**
     * Sets the {@link DeliveryGuarantee} to provide for writing to Snowflake.
     *
     * @param deliveryGuarantee {@link DeliveryGuarantee}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> deliveryGuarantee(final DeliveryGuarantee deliveryGuarantee) {
        this.writerConfigBuilder.deliveryGuarantee(deliveryGuarantee);
        return this;
    }

    /**
     * Sets the timeout duration for waiting for committed offsets to align during flush operations.
     * This is the maximum time the sink will wait for Snowflake to confirm that all buffered
     * records have been committed before failing the checkpoint.
     *
     * <p>Default: {@link SnowflakeWriterConfig#COMMIT_TIMEOUT_DEFAULT} (5 minutes)
     *
     * @param commitTimeoutMs timeout milliseconds, must be non-negative, a value of 0 indicates an
     *     infinite timeout
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> commitTimeoutMs(final long commitTimeoutMs) {
        this.writerConfigBuilder.commitTimeoutMs(commitTimeoutMs);
        return this;
    }

    /**
     * Sets the timeout duration for closing a channel. This is the maximum time the sink will wait
     * for the channel to flush all buffered data when closing.
     *
     * <p>Default: {@link SnowflakeChannelConfig#CHANNEL_CLOSE_TIMEOUT_MS_DEFAULT} (5 seconds)
     *
     * @param channelCloseTimeoutMs timeout milliseconds, must be non-negative, a value of 0
     *     indicates an infinite timeout
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> channelCloseTimeoutMs(final long channelCloseTimeoutMs) {
        this.channelConfigBuilder.channelCloseTimeoutMs(channelCloseTimeoutMs);
        return this;
    }

    /**
     * Sets the serialization schema that provides serialization from {@link IN} to {@link
     * java.util.Map} row as documented above and by the Snowflake service.
     *
     * @param serializationSchema {@link SnowflakeRowSerializationSchema}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> serializationSchema(
            final SnowflakeRowSerializationSchema<IN> serializationSchema) {
        Preconditions.checkState(
                InstantiationUtil.isSerializable(Preconditions.checkNotNull(serializationSchema)),
                "The implementation for Snowflake row serialization must be serializable");
        this.serializationSchema = serializationSchema;
        return this;
    }

    // ====================================================================
    // Observability configuration
    // ====================================================================

    /**
     * Configures observability settings for the Snowflake Streaming Ingest SDK. Use this method to
     * configure metrics collection and logging levels.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * SnowflakeSink.<T>builder()
     *     .observability(obs -> obs
     *         .enableMetrics()
     *         .metricsPort(50000)
     *         .metricsIp("0.0.0.0")
     *         .logLevel(LogLevel.INFO))
     *     .build("my-app-id");
     * }</pre>
     *
     * @param observabilityConfigurer a consumer that configures the {@link
     *     ObservabilityConfig.ObservabilityConfigBuilder}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> observability(
            final ObservabilityConfig.ObservabilityConfigBuilder observabilityConfigurer) {
        Preconditions.checkNotNull(observabilityConfigurer, "observabilityConfigurer");
        this.clientConfigBuilder.observability(
                ObservabilityConfig.ObservabilityConfigBuilder::build);
        return this;
    }

    /**
     * Creates a {@link SnowflakeSink} with provided configuration.
     *
     * @return {@link SnowflakeSink}
     */
    public SnowflakeSink<IN> build(final String appId) {
        return new SnowflakeSink<>(
                appId,
                this.clientConfigBuilder.build(),
                this.writerConfigBuilder.build(),
                this.channelConfigBuilder.build(this.database, this.schema, this.table),
                Preconditions.checkNotNull(
                        this.serializationSchema, "A serialization schema must be provided"));
    }
}
