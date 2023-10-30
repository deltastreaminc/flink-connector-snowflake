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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;
import net.snowflake.ingest.streaming.OpenChannelRequest;

import java.util.List;
import java.util.Properties;

/**
 * Builder for constructing a {@link SnowflakeSink} with appropriate configurations.
 *
 * @param <IN> type of the events produced by Flink
 */
@PublicEvolving
public class SnowflakeSinkBuilder<IN> {

    static final String SNOWFLAKE_URL_CONFIG_NAME = "url";
    static final String SNOWFLAKE_USER_CONFIG_NAME = "user";
    static final String SNOWFLAKE_ROLE_CONFIG_NAME = "role";
    static final String SNOWFLAKE_PRIVATE_KEY_CONFIG_NAME = "private_key";
    static final String SNOWFLAKE_KEY_PASSPHRASE_CONFIG_NAME = "snowflake.private.key.passphrase";

    /**
     * At minimum, needs to include the required properties as documented by Snowflake: <a
     * href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-configuration#required-properties">Required
     * connection settings</a>.
     */
    private final Properties connectionProps = new Properties();

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
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(connectionUrl),
                String.format("Invalid %s", SNOWFLAKE_URL_CONFIG_NAME));
        this.connectionProps.put(SNOWFLAKE_URL_CONFIG_NAME, connectionUrl);
        return this;
    }

    /**
     * Set the user connecting to the Snowflake service.
     *
     * @param connectionUser {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> user(final String connectionUser) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(connectionUser),
                String.format("Invalid %s", SNOWFLAKE_USER_CONFIG_NAME));
        this.connectionProps.put(SNOWFLAKE_USER_CONFIG_NAME, connectionUser);
        return this;
    }

    /**
     * Set the role as to connect to the Snowflake service.
     *
     * @param connectionRole {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> role(final String connectionRole) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(connectionRole),
                String.format("Invalid %s", SNOWFLAKE_ROLE_CONFIG_NAME));
        this.connectionProps.put(SNOWFLAKE_ROLE_CONFIG_NAME, connectionRole);
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
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(connectionPrivateKey),
                String.format("Invalid %s", SNOWFLAKE_PRIVATE_KEY_CONFIG_NAME));
        this.connectionProps.put(SNOWFLAKE_PRIVATE_KEY_CONFIG_NAME, connectionPrivateKey);
        return this;
    }

    /**
     * Set the private key password for the private key in {@link #privateKey(String)}.
     *
     * @param connectionKeyPassphrase {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> keyPassphrase(final String connectionKeyPassphrase) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(connectionKeyPassphrase),
                String.format("Invalid %s", SNOWFLAKE_KEY_PASSPHRASE_CONFIG_NAME));
        this.connectionProps.put(SNOWFLAKE_KEY_PASSPHRASE_CONFIG_NAME, connectionKeyPassphrase);
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
        this.database = database;
        return this;
    }

    /**
     * Set the schema name to sink to in Snowflake.
     *
     * @param schema {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> schema(final String schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Set the table name to sink to in Snowflake.
     *
     * @param table {@link java.lang.String}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * Set the option for handling errors within a Snowflake ingest channel.
     *
     * @param option {@link net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> onErrorOption(final OpenChannelRequest.OnErrorOption option) {
        this.channelConfigBuilder.onErrorOption(option);
        return this;
    }

    /**
     * Sets the maximum time, in milliseconds, to buffer incoming elements.
     *
     * @param bufferTimeMillis {@link long}
     * @return {@code this}
     */
    public SnowflakeSinkBuilder<IN> bufferTimeMillis(final long bufferTimeMillis) {
        this.writerConfigBuilder.maxBufferTimeMs(bufferTimeMillis);
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

    /**
     * Creates a {@link SnowflakeSink} with provided configuration.
     *
     * @return {@link SnowflakeSink}
     */
    public SnowflakeSink<IN> build(final String appId) {
        this.checkConnectionProps();

        return new SnowflakeSink<>(
                appId,
                this.connectionProps,
                this.writerConfigBuilder.build(),
                this.channelConfigBuilder.build(this.database, this.schema, this.table),
                Preconditions.checkNotNull(
                        this.serializationSchema, "A serialization schema must be provided"));
    }

    @VisibleForTesting
    void checkConnectionProps() {

        // check the minimum connectivity settings
        Preconditions.checkArgument(
                this.connectionProps
                        .keySet()
                        .containsAll(
                                List.of(
                                        SNOWFLAKE_URL_CONFIG_NAME,
                                        SNOWFLAKE_USER_CONFIG_NAME,
                                        SNOWFLAKE_ROLE_CONFIG_NAME)),
                "Required connection properties documented by Snowflake must be set");

        // key passphrase requires private key
        if (this.connectionProps.containsKey(SNOWFLAKE_KEY_PASSPHRASE_CONFIG_NAME)) {
            Preconditions.checkArgument(
                    this.connectionProps.containsKey(SNOWFLAKE_PRIVATE_KEY_CONFIG_NAME),
                    "%s requires %s",
                    SNOWFLAKE_KEY_PASSPHRASE_CONFIG_NAME,
                    SNOWFLAKE_PRIVATE_KEY_CONFIG_NAME);
        }
    }
}
