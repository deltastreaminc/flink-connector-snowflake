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
import org.apache.flink.util.Preconditions;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import io.deltastream.flink.connector.snowflake.sink.config.ObservabilityConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param config {@link SnowflakeClientConfig} All required and optional Snowflake client
     *     configs
     * @return {@link SnowflakeStreamingIngestClient} A new ingestion client
     */
    public static SnowflakeStreamingIngestClient createClient(
            final String ingestName,
            final String databaseName,
            final String schemaName,
            final String tableName,
            final SnowflakeClientConfig config) {

        // set observability system properties before creating the client
        configureObservability(config.getObservabilityConfig());

        final String ingestClientName =
                SnowflakeInternalUtils.createClientOrChannelName(
                        STREAMING_INGEST_CLIENT_PREFIX_NAME, ingestName, null);

        SnowflakeStreamingIngestClient ingestClient =
                SnowflakeStreamingIngestClientFactory.builder(
                                ingestClientName,
                                databaseName,
                                schemaName,
                                String.format("%s-STREAMING", tableName))
                        .setProperties(config.getConnectionProps())
                        .build();

        LOGGER.info(
                "Successfully initialized Snowflake streaming ingest client {}", ingestClientName);
        return ingestClient;
    }

    /**
     * Sets the observability configuration as system properties. The Snowflake SDK checks system
     * properties before environment variables, so setting them here allows the configuration to be
     * controlled programmatically.
     *
     * @param observabilityConfig {@link ObservabilityConfig}
     */
    private static void configureObservability(final ObservabilityConfig observabilityConfig) {
        Preconditions.checkNotNull(observabilityConfig, "observabilityConfig");

        System.setProperty(
                ClientOptions.Observability.ENABLE_METRICS_KEY,
                String.valueOf(observabilityConfig.isEnableMetrics()).toUpperCase());
        System.setProperty(
                ClientOptions.Observability.METRICS_PORT_KEY,
                String.valueOf(observabilityConfig.getMetricsPort()));
        System.setProperty(
                ClientOptions.Observability.METRICS_IP_KEY, observabilityConfig.getMetricsIp());
        System.setProperty(
                ClientOptions.Observability.LOG_LEVEL_KEY, observabilityConfig.getLogLevelValue());

        LOGGER.info(
                "Set Snowflake SDK observability system properties: [{}={}, {}={}, {}={}, {}={}]",
                ClientOptions.Observability.ENABLE_METRICS_KEY,
                observabilityConfig.isEnableMetrics(),
                ClientOptions.Observability.METRICS_PORT_KEY,
                observabilityConfig.getMetricsPort(),
                ClientOptions.Observability.METRICS_IP_KEY,
                observabilityConfig.getMetricsIp(),
                ClientOptions.Observability.LOG_LEVEL_KEY,
                observabilityConfig.getLogLevelValue());
    }
}
