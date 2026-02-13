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

package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.annotation.Internal;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Facade to provide the underlying {@link SnowflakeStreamingIngestClient}. The lifecycle of the
 * created client is not managed in this class.
 */
@Internal
public class SnowflakeStreamingIngestClientProvider {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SnowflakeStreamingIngestClientProvider.class);

    private static final String STREAMING_INGEST_CLIENT_PREFIX_NAME = "FLINK_INGEST_CLIENT";

    /**
     * Created based on the Snowflake client APIs and configuration. See <a
     * href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations">
     * Configuring Snowpipe Streaming</a>
     *
     * @param ingestName {@link java.lang.String} a name to uniquely identify the upstream
     *     application
     * @param connectorProps {@link java.util.Properties} All required and optional Snowflake
     *     streaming configs
     * @return {@link SnowflakeStreamingIngestClient} A new ingestion client
     */
    public static SnowflakeStreamingIngestClient createClient(
            final String ingestName,
            final String databaseName,
            final String schemaName,
            final String tableName,
            final Properties connectorProps) {
        final String ingestClientName =
                SnowflakeInternalUtils.createClientOrChannelName(
                        STREAMING_INGEST_CLIENT_PREFIX_NAME, ingestName, null);

        SnowflakeStreamingIngestClient ingestClient =
                SnowflakeStreamingIngestClientFactory.builder(
                                ingestClientName,
                                databaseName,
                                schemaName,
                                String.format("%s-STREAMING", tableName))
                        .setProperties(connectorProps)
                        .build();

        LOGGER.info(
                "Successfully initialized Snowflake streaming ingest client {}", ingestClientName);
        return ingestClient;
    }
}
