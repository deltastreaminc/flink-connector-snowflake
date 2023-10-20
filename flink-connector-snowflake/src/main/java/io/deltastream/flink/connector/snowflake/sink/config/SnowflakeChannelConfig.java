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

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class provides the configuration needed to create a {@link
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel}, e.g. database/schema/table name,
 * etc.
 */
@PublicEvolving
public final class SnowflakeChannelConfig implements Serializable {

    private static final long serialVersionUID = 3517937247835076255L;

    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final OpenChannelRequest.OnErrorOption onErrorOption;

    public String getDatabaseName() {
        return databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public OnErrorOption getOnErrorOption() {
        return onErrorOption;
    }

    private SnowflakeChannelConfig(SnowflakeChannelConfigBuilder builder) {
        this.databaseName = builder.databaseName;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.onErrorOption = builder.onErrorOption;
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
                && Objects.equals(this.getTableName(), that.getTableName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getDatabaseName(), this.getSchemaName(), this.getTableName());
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
        private OpenChannelRequest.OnErrorOption onErrorOption =
                OpenChannelRequest.OnErrorOption.ABORT;

        public SnowflakeChannelConfigBuilder onErrorOption(
                final OpenChannelRequest.OnErrorOption onErrorOption) {
            this.onErrorOption = Preconditions.checkNotNull(onErrorOption, "onErrorOption");
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
