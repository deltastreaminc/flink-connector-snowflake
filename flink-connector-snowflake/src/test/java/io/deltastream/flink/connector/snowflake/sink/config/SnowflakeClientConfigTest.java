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

import io.deltastream.flink.connector.snowflake.sink.internal.ClientOptions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link SnowflakeClientConfig}. */
class SnowflakeClientConfigTest {

    @Test
    void testBuildWithExplicitAccount() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("explicit_account")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("explicit_account");
    }

    @Test
    void testBuildExtractsAccountFromUrl() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildExtractsAccountFromUrlWithPort() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com:443")
                        .user("testuser")
                        .role("testrole")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildExtractsAccountFromUrlWithPath() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com/path/to/resource")
                        .user("testuser")
                        .role("testrole")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildExtractsAccountFromUrlWithoutProtocol() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildExplicitAccountOverridesExtraction() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("explicit_override")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("explicit_override");
    }

    @Test
    void testBuildFailsWhenAccountIdIsNotPresent() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://invalid.otherdomain.com")
                                        .user("testuser")
                                        .role("testrole")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Account ID must be either explicitly provided or extractable from the URL");
    }

    @Test
    void testBuildFailsWithoutUrl() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .user("testuser")
                                        .role("testrole")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Required connection properties");
    }

    @Test
    void testBuildFailsWithoutUser() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://myorg-myaccount.snowflakecomputing.com")
                                        .role("testrole")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Required connection properties");
    }

    @Test
    void testBuildFailsWithoutRole() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://myorg-myaccount.snowflakecomputing.com")
                                        .user("testuser")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Required connection properties");
    }

    @Test
    void testBuildWithPrivateKey() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .privateKey("test-private-key")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.PRIVATE_KEY.key()))
                .isEqualTo("test-private-key");
        // Verify account ID was extracted from URL
        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildWithPrivateKeyAndPassphrase() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .privateKey("test-private-key")
                        .keyPassphrase("test-passphrase")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.PRIVATE_KEY.key()))
                .isEqualTo("test-private-key");
        Assertions.assertThat(
                        config.getConnectionProps()
                                .getProperty(ClientOptions.PRIVATE_KEY_PASSPHRASE.key()))
                .isEqualTo("test-passphrase");
        // Verify account ID was extracted from URL
        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
    }

    @Test
    void testBuildFailsWithPassphraseButNoPrivateKey() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://myorg-myaccount.snowflakecomputing.com")
                                        .user("testuser")
                                        .role("testrole")
                                        .keyPassphrase("test-passphrase")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("private_key_passphrase")
                .hasMessageContaining("private_key");
    }

    @Test
    void testBuildFailsWithBlankAccount() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://myorg-myaccount.snowflakecomputing.com")
                                        .user("testuser")
                                        .role("testrole")
                                        .accountId("   ")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid accountId");
    }

    @Test
    void testBuildWithDefaultObservabilityConfig() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
                        .build();

        Assertions.assertThat(config.getObservabilityConfig()).isNotNull();
        Assertions.assertThat(config.getObservabilityConfig().isEnableMetrics()).isFalse();
    }

    @Test
    void testBuildWithCustomObservabilityConfig() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
                        .observability(
                                obs ->
                                        obs.enableMetrics()
                                                .metricsPort(9090)
                                                .metricsIp("0.0.0.0")
                                                .logLevel(ObservabilityConfig.LogLevel.WARN))
                        .build();

        ObservabilityConfig obsConfig = config.getObservabilityConfig();
        Assertions.assertThat(obsConfig).isNotNull();
        Assertions.assertThat(obsConfig.isEnableMetrics()).isTrue();
        Assertions.assertThat(obsConfig.getMetricsPort()).isEqualTo(9090);
        Assertions.assertThat(obsConfig.getMetricsIp()).isEqualTo("0.0.0.0");
        Assertions.assertThat(obsConfig.getLogLevel()).isEqualTo(ObservabilityConfig.LogLevel.WARN);
    }

    @Test
    void testBuildWithObservabilityOnlyMetricsEnabled() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
                        .observability(obs -> obs.enableMetrics())
                        .build();

        ObservabilityConfig obsConfig = config.getObservabilityConfig();
        Assertions.assertThat(obsConfig.isEnableMetrics()).isTrue();
        // Other values should have defaults
        Assertions.assertThat(obsConfig.getMetricsPort())
                .isEqualTo(ClientOptions.Observability.METRICS_PORT_DEFAULT);
        Assertions.assertThat(obsConfig.getMetricsIp())
                .isEqualTo(ClientOptions.Observability.METRICS_IP_DEFAULT);
        Assertions.assertThat(obsConfig.getLogLevel())
                .isEqualTo(ClientOptions.Observability.LOG_LEVEL_DEFAULT);
    }

    @Test
    void testBuildWithObservabilityOnlyLogLevel() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
                        .observability(obs -> obs.logLevel(ObservabilityConfig.LogLevel.ERROR))
                        .build();

        ObservabilityConfig obsConfig = config.getObservabilityConfig();
        Assertions.assertThat(obsConfig.getLogLevel())
                .isEqualTo(ObservabilityConfig.LogLevel.ERROR);
        Assertions.assertThat(obsConfig.isEnableMetrics()).isFalse(); // default
    }

    @Test
    void testBuildWithNullObservabilityConfigurer() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://myorg-myaccount.snowflakecomputing.com")
                                        .user("testuser")
                                        .role("testrole")
                                        .accountId("myorg-myaccount")
                                        .observability(null)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("observabilityConfigurer");
    }

    @Test
    void testBuildWithAllConnectionAndObservabilityOptions() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
                        .privateKey("test-private-key")
                        .keyPassphrase("test-passphrase")
                        .observability(
                                obs ->
                                        obs.enableMetrics()
                                                .metricsPort(50000)
                                                .metricsIp("127.0.0.1")
                                                .logLevel(ObservabilityConfig.LogLevel.INFO))
                        .build();

        // Verify connection properties
        Assertions.assertThat(config.getConnectionProps().getProperty(ClientOptions.URL.key()))
                .isEqualTo("https://myorg-myaccount.snowflakecomputing.com");
        Assertions.assertThat(config.getConnectionProps().getProperty(ClientOptions.USER.key()))
                .isEqualTo("testuser");
        Assertions.assertThat(config.getConnectionProps().getProperty(ClientOptions.ROLE.key()))
                .isEqualTo("testrole");
        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.ACCOUNT_ID.key()))
                .isEqualTo("myorg-myaccount");
        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.PRIVATE_KEY.key()))
                .isEqualTo("test-private-key");
        Assertions.assertThat(
                        config.getConnectionProps()
                                .getProperty(ClientOptions.PRIVATE_KEY_PASSPHRASE.key()))
                .isEqualTo("test-passphrase");

        // Verify observability config
        ObservabilityConfig obsConfig = config.getObservabilityConfig();
        Assertions.assertThat(obsConfig.isEnableMetrics()).isTrue();
        Assertions.assertThat(obsConfig.getMetricsPort()).isEqualTo(50000);
        Assertions.assertThat(obsConfig.getMetricsIp()).isEqualTo("127.0.0.1");
        Assertions.assertThat(obsConfig.getLogLevel()).isEqualTo(ObservabilityConfig.LogLevel.INFO);
    }
}
