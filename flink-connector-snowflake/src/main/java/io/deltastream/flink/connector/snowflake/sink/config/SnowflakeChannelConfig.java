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

package io.deltastream.flink.connector.snowflake.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * This class provides the configuration needed to create a {@link
 * com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel}, e.g. database/schema/table name,
 * etc.
 */
@PublicEvolving
public final class SnowflakeChannelConfig implements Serializable {

    private static final long serialVersionUID = 3517937247835076255L;

    /** Default timeout for closing a channel. */
    public static final long CHANNEL_CLOSE_TIMEOUT_MS_DEFAULT = 5_000L;

    /**
     * Table name parts are treated as case-insensitive by Snowflake, meaning that the service will
     * treat them as UPPER_CASE, unless surrounded by double quotes, e.g. `DB.SCHEMA."table"`. The
     * same name without double quotes will be replaced by its uppercase version, e.g.
     * `DB.SCHEMA.TABLE`.
     */
    private final String databaseName;

    private final String schemaName;
    private final String tableName;
    private final Duration channelCloseTimeout;

    public String getDatabaseName() {
        return databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Duration getChannelCloseTimeout() {
        return channelCloseTimeout;
    }

    private SnowflakeChannelConfig(SnowflakeChannelConfigBuilder builder) {
        this.databaseName = builder.databaseName;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.channelCloseTimeout = builder.channelCloseTimeoutMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnowflakeChannelConfig that = (SnowflakeChannelConfig) o;
        return Objects.equals(this.getDatabaseName(), that.getDatabaseName())
                && Objects.equals(this.getSchemaName(), that.getSchemaName())
                && Objects.equals(this.getTableName(), that.getTableName())
                && Objects.equals(this.getChannelCloseTimeout(), that.getChannelCloseTimeout());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.getDatabaseName(),
                this.getSchemaName(),
                this.getTableName(),
                this.getChannelCloseTimeout());
    }

    public static SnowflakeChannelConfigBuilder builder() {
        return new SnowflakeChannelConfigBuilder();
    }

    /** Builder for {@link SnowflakeChannelConfig}. */
    @PublicEvolving
    public static class SnowflakeChannelConfigBuilder {

        private String databaseName;
        private String schemaName;
        private String tableName;
        private Duration channelCloseTimeoutMs =
                Duration.ofMillis(CHANNEL_CLOSE_TIMEOUT_MS_DEFAULT);

        /**
         * Set the timeout duration for closing a channel. This is the maximum time the sink will
         * wait for the channel to flush all buffered data when closing.
         *
         * <p>Default: 5 seconds
         *
         * @param channelCloseTimeoutMs timeout milliseconds, must be non-negative, a value of 0
         *     indicates an infinite timeout
         * @return {@code this}
         */
        public SnowflakeChannelConfigBuilder channelCloseTimeoutMs(
                final long channelCloseTimeoutMs) {
            Preconditions.checkNotNull(
                    channelCloseTimeoutMs, "channelCloseTimeoutMs cannot be null");
            Preconditions.checkArgument(
                    channelCloseTimeoutMs >= 0, "channelCloseTimeoutMs must be non-negative");

            if (channelCloseTimeoutMs == 0L) {
                this.channelCloseTimeoutMs = Duration.ofMillis(Integer.MAX_VALUE);
            } else {
                this.channelCloseTimeoutMs = Duration.ofMillis(channelCloseTimeoutMs);
            }
            return this;
        }

        /**
         * Build a {@link SnowflakeChannelConfig} from user-provided channel configurations.
         *
         * @return {@link SnowflakeChannelConfig}
         */
        public SnowflakeChannelConfig build(
                final String databaseName, final String schemaName, final String tableName) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(databaseName), "Invalid database name");
            this.databaseName = databaseName;
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(schemaName), "Invalid schema name");
            this.schemaName = schemaName;
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(tableName), "Invalid table name");
            this.tableName = tableName;
            return new SnowflakeChannelConfig(this);
        }

        private SnowflakeChannelConfigBuilder() {}
    }
}
