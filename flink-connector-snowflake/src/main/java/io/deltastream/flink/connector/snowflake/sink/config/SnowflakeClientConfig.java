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

import io.deltastream.flink.connector.snowflake.sink.internal.ClientOptions;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * This class provides the configuration needed to create a Snowflake client connection. It handles
 * connection properties such as URL, user, role, private key, and accountId information.
 */
@PublicEvolving
public final class SnowflakeClientConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Properties connectionProps;
    private final ObservabilityConfig observabilityConfig;

    public Properties getConnectionProps() {
        return connectionProps;
    }

    public ObservabilityConfig getObservabilityConfig() {
        return observabilityConfig;
    }

    private SnowflakeClientConfig(SnowflakeClientConfigBuilder builder) {
        this.connectionProps = builder.connectionProps;
        this.observabilityConfig = builder.observabilityConfig;
    }

    public static SnowflakeClientConfigBuilder builder() {
        return new SnowflakeClientConfigBuilder();
    }

    /** Builder for {@link SnowflakeClientConfig}. */
    @PublicEvolving
    public static class SnowflakeClientConfigBuilder {

        private final Properties connectionProps = new Properties();
        private ObservabilityConfig observabilityConfig = ObservabilityConfig.builder().build();

        /**
         * Set the connection URL for connecting to the Snowflake service.
         *
         * @param connectionUrl {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder url(final String connectionUrl) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(connectionUrl),
                    String.format("Invalid %s", ClientOptions.URL.key()));
            this.connectionProps.put(ClientOptions.URL.key(), connectionUrl);
            return this;
        }

        /**
         * Set the user connecting to the Snowflake service.
         *
         * @param connectionUser {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder user(final String connectionUser) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(connectionUser),
                    String.format("Invalid %s", ClientOptions.USER.key()));
            this.connectionProps.put(ClientOptions.USER.key(), connectionUser);
            return this;
        }

        /**
         * Set the role as to connect to the Snowflake service.
         *
         * @param connectionRole {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder role(final String connectionRole) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(connectionRole),
                    String.format("Invalid %s", ClientOptions.ROLE.key()));
            this.connectionProps.put(ClientOptions.ROLE.key(), connectionRole);
            return this;
        }

        /**
         * Set the account identifier for the Snowflake service.
         *
         * @param account {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder accountId(final String account) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(account), "Invalid accountId");
            // TODO make optional by parsing accountId from URL, if not provided explicitly
            this.connectionProps.put(ClientOptions.ACCOUNT_ID.key(), account);
            return this;
        }

        /**
         * Set the private key to connect with to the Snowflake service. The private key must only
         * include the key content without any header, footer, or newline feeds.
         *
         * @param connectionPrivateKey {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder privateKey(final String connectionPrivateKey) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(connectionPrivateKey),
                    String.format("Invalid %s", ClientOptions.PRIVATE_KEY.key()));
            this.connectionProps.put(ClientOptions.PRIVATE_KEY.key(), connectionPrivateKey);
            return this;
        }

        /**
         * Set the private key password for the private key in {@link #privateKey(String)}.
         *
         * @param connectionKeyPassphrase {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder keyPassphrase(final String connectionKeyPassphrase) {
            Preconditions.checkArgument(
                    connectionKeyPassphrase != null,
                    String.format("Invalid %s", ClientOptions.PRIVATE_KEY_PASSPHRASE.key()));
            this.connectionProps.put(
                    ClientOptions.PRIVATE_KEY_PASSPHRASE.key(), connectionKeyPassphrase);
            return this;
        }

        // ====================================================================
        // Observability configuration
        // ====================================================================

        /**
         * Configures observability settings for the Snowflake Streaming Ingest SDK. Use this method
         * to configure metrics collection and logging levels.
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
        public SnowflakeClientConfigBuilder observability(
                final Consumer<ObservabilityConfig.ObservabilityConfigBuilder>
                        observabilityConfigurer) {
            Preconditions.checkNotNull(observabilityConfigurer, "observabilityConfigurer");
            ObservabilityConfig.ObservabilityConfigBuilder tmpBuilder =
                    ObservabilityConfig.builder();
            observabilityConfigurer.accept(tmpBuilder);
            this.observabilityConfig = tmpBuilder.build();
            return this;
        }

        /**
         * Build a {@link SnowflakeClientConfig} from user-provided client configurations. This
         * method validates the connection properties to ensure all required settings are present.
         *
         * @return {@link SnowflakeClientConfig}
         */
        public SnowflakeClientConfig build() {
            this.checkConnectionProps();
            return new SnowflakeClientConfig(this);
        }

        /**
         * Validates the connection properties to ensure all required settings are present. Required
         * properties: URL, USER, and ROLE. Also validates that if key passphrase is provided,
         * private key must also be present.
         */
        void checkConnectionProps() {

            // check the minimum connectivity settings
            Preconditions.checkArgument(
                    this.connectionProps
                            .keySet()
                            .containsAll(
                                    List.of(
                                            ClientOptions.URL.key(),
                                            ClientOptions.USER.key(),
                                            ClientOptions.ROLE.key(),
                                            ClientOptions.ACCOUNT_ID.key())),
                    "Required connection properties documented by Snowflake must be set, provided properties: %s",
                    this.connectionProps.keySet());

            // key passphrase requires private key
            if (this.connectionProps.containsKey(ClientOptions.PRIVATE_KEY_PASSPHRASE.key())) {
                Preconditions.checkArgument(
                        this.connectionProps.containsKey(ClientOptions.PRIVATE_KEY.key()),
                        "%s requires %s",
                        ClientOptions.PRIVATE_KEY_PASSPHRASE.key(),
                        ClientOptions.PRIVATE_KEY.key());
            }
        }

        private SnowflakeClientConfigBuilder() {}
    }
}
