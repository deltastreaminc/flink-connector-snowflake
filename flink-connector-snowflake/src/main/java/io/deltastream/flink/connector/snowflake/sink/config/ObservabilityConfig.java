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

import io.deltastream.flink.connector.snowflake.sink.internal.ClientOptions;

import java.io.Serializable;

/**
 * This class provides the observability configuration for the Snowflake Streaming Ingest SDK. It
 * handles metrics and logging settings that will be set as system properties before the client is
 * created.
 *
 * <p>See <a
 * href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations#environment-variables">Snowflake
 * Environment Variables</a>
 */
@PublicEvolving
public final class ObservabilityConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean enableMetrics;
    private final int metricsPort;
    private final String metricsIp;
    private final LogLevel logLevel;

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public String getMetricsIp() {
        return metricsIp;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    /**
     * Gets the string value of the log level for setting as a system property.
     *
     * @return the log level string value
     */
    public String getLogLevelValue() {
        return logLevel.getValue();
    }

    private ObservabilityConfig(ObservabilityConfigBuilder builder) {
        this.enableMetrics = builder.enableMetrics;
        this.metricsPort = builder.metricsPort;
        this.metricsIp = builder.metricsIp;
        this.logLevel = builder.logLevel;
    }

    /**
     * Creates a new builder for {@link ObservabilityConfig}.
     *
     * @return a new builder instance
     */
    public static ObservabilityConfigBuilder builder() {
        return new ObservabilityConfigBuilder();
    }

    /**
     * Builder for {@link ObservabilityConfig}. This builder can be used to configure observability
     * settings for the Snowflake Streaming Ingest SDK.
     */
    @PublicEvolving
    public static class ObservabilityConfigBuilder {

        private boolean enableMetrics = ClientOptions.Observability.ENABLE_METRICS_DEFAULT;
        private int metricsPort = ClientOptions.Observability.METRICS_PORT_DEFAULT;
        private String metricsIp = ClientOptions.Observability.METRICS_IP_DEFAULT;
        private LogLevel logLevel = ClientOptions.Observability.LOG_LEVEL_DEFAULT;

        /**
         * Enable or disable metrics collection for the Snowflake Streaming Ingest SDK.
         *
         * @return {@code this}
         */
        public ObservabilityConfigBuilder enableMetrics() {
            this.enableMetrics = true;
            return this;
        }

        /**
         * Set the port number for exposing Snowflake SDK metrics.
         *
         * @param metricsPort port number (must be between 1 and 65535)
         * @return {@code this}
         */
        public ObservabilityConfigBuilder metricsPort(final int metricsPort) {
            Preconditions.checkArgument(
                    metricsPort > 0 && metricsPort <= 65535,
                    "Metrics port must be between 1 and 65535");
            this.metricsPort = metricsPort;
            return this;
        }

        /**
         * Set the IP address for exposing Snowflake SDK metrics.
         *
         * @param metricsIp IP address string
         * @return {@code this}
         */
        public ObservabilityConfigBuilder metricsIp(final String metricsIp) {
            this.metricsIp = Preconditions.checkNotNull(metricsIp, "metricsIp cannot be null");
            return this;
        }

        /**
         * Set the log level for the Snowflake Streaming Ingest SDK using the {@link LogLevel} enum.
         *
         * @param logLevel the log level enum value
         * @return {@code this}
         */
        public ObservabilityConfigBuilder logLevel(final LogLevel logLevel) {
            this.logLevel = Preconditions.checkNotNull(logLevel, "logLevel cannot be null");
            return this;
        }

        /**
         * Build a {@link ObservabilityConfig} from user-provided observability configurations.
         *
         * @return {@link ObservabilityConfig}
         */
        public ObservabilityConfig build() {
            return new ObservabilityConfig(this);
        }

        private ObservabilityConfigBuilder() {}
    }

    /**
     * Log level options for the Snowflake Streaming Ingest SDK.
     *
     * <p>These levels control the verbosity of logging from the Snowflake SDK. See <a
     * href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations#environment-variables">Snowflake
     * Environment Variables</a>
     */
    @PublicEvolving
    public enum LogLevel {
        /** Informational messages - most verbose level. */
        INFO("info"),

        /** Warning messages only. */
        WARN("warn"),

        /** Error messages only - least verbose level. */
        ERROR("error");

        private final String value;

        LogLevel(String value) {
            this.value = value;
        }

        /**
         * Gets the string value of the log level that will be set as a system property.
         *
         * @return the log level string value
         */
        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

        /**
         * Parse a string value into a LogLevel enum.
         *
         * @param value the string value (case-insensitive)
         * @return the corresponding LogLevel
         * @throws IllegalArgumentException if the value is not a valid log level
         */
        public static LogLevel fromString(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Log level cannot be null");
            }
            String lowerValue = value.toLowerCase();
            for (LogLevel level : LogLevel.values()) {
                if (level.value.equals(lowerValue)) {
                    return level;
                }
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid log level: %s. Valid values are: info, warn, error", value));
        }
    }
}
