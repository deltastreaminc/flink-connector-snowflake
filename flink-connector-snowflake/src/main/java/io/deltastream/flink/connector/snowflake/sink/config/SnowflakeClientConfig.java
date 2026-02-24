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
         * Set the account identifier for the Snowflake service. This is optional if the URL follows
         * the format {@code https://<account_id>.snowflakecomputing.com}, as the account ID will be
         * automatically extracted from the URL.
         *
         * @param account {@link java.lang.String}
         * @return {@code this}
         */
        public SnowflakeClientConfigBuilder accountId(final String account) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(account), "Invalid accountId");
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
         * If accountId is not explicitly provided, it will be extracted from the URL.
         *
         * @return {@link SnowflakeClientConfig}
         */
        public SnowflakeClientConfig build() {
            // Extract accountId from URL if not explicitly provided
            if (!this.connectionProps.containsKey(ClientOptions.ACCOUNT_ID.key())
                    && this.connectionProps.containsKey(ClientOptions.URL.key())) {
                String url = this.connectionProps.getProperty(ClientOptions.URL.key());
                String extractedAccountId = this.extractAccountIdFromUrl(url);
                if (extractedAccountId != null) {
                    this.connectionProps.put(ClientOptions.ACCOUNT_ID.key(), extractedAccountId);
                }
            }
            this.checkConnectionProps();
            return new SnowflakeClientConfig(this);
        }

        /**
         * Validates the connection properties to ensure all required settings are present. Required
         * properties: URL, USER, and ROLE. ACCOUNT_ID is optional as it can be extracted from URL.
         * Also validates that if key passphrase is provided, private key must also be present.
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
                                            ClientOptions.ROLE.key())),
                    "Required connection properties documented by Snowflake must be set, provided properties: %s",
                    this.connectionProps.keySet());

            // Ensure accountId is present (either explicitly set or extracted from URL)
            Preconditions.checkArgument(
                    this.connectionProps.containsKey(ClientOptions.ACCOUNT_ID.key()),
                    "Account ID must be either explicitly provided or extractable from the URL. "
                            + "Expected URL format: https://<account_id>.snowflakecomputing.com");

            // key passphrase requires private key
            if (this.connectionProps.containsKey(ClientOptions.PRIVATE_KEY_PASSPHRASE.key())) {
                Preconditions.checkArgument(
                        this.connectionProps.containsKey(ClientOptions.PRIVATE_KEY.key()),
                        "%s requires %s",
                        ClientOptions.PRIVATE_KEY_PASSPHRASE.key(),
                        ClientOptions.PRIVATE_KEY.key());
            }
        }

        /**
         * Extracts the account identifier from a Snowflake URL. Expected URL format: {@code
         * https://<account_id>.snowflakecomputing.com}
         *
         * @param url the Snowflake connection URL
         * @return the extracted account ID, or null if it cannot be extracted
         */
        private String extractAccountIdFromUrl(String url) {
            if (StringUtils.isNullOrWhitespaceOnly(url)) {
                return null;
            }

            try {
                // Remove protocol if present
                String cleanUrl = url.toLowerCase().trim();
                if (cleanUrl.startsWith("https://")) {
                    cleanUrl = cleanUrl.substring(8);
                } else if (cleanUrl.startsWith("http://")) {
                    cleanUrl = cleanUrl.substring(7);
                }

                // Remove port if present
                int portIndex = cleanUrl.indexOf(':');
                if (portIndex > 0) {
                    cleanUrl = cleanUrl.substring(0, portIndex);
                }

                // Remove path if present
                int pathIndex = cleanUrl.indexOf('/');
                if (pathIndex > 0) {
                    cleanUrl = cleanUrl.substring(0, pathIndex);
                }

                // Extract account ID (part before .snowflakecomputing.com)
                if (cleanUrl.contains(".snowflakecomputing.com")) {
                    int dotIndex = cleanUrl.indexOf(".snowflakecomputing.com");
                    return cleanUrl.substring(0, dotIndex);
                }
            } catch (Exception e) {
                // If any parsing error occurs, return null to let validation handle it
                return null;
            }

            return null;
        }

        private SnowflakeClientConfigBuilder() {}
    }
}
