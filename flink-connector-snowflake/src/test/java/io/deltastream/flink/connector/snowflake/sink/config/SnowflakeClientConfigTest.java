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
    void testBuildFailsWhenAccountIdIsNotPresent() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeClientConfig.builder()
                                        .url("https://invalid.otherdomain.com")
                                        .user("testuser")
                                        .role("testrole")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Required connection properties documented")
                .hasMessageContaining("[role, user, url]");
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
                        .accountId("myorg-myaccount")
                        .privateKey("test-private-key")
                        .build();

        Assertions.assertThat(
                        config.getConnectionProps().getProperty(ClientOptions.PRIVATE_KEY.key()))
                .isEqualTo("test-private-key");
    }

    @Test
    void testBuildWithPrivateKeyAndPassphrase() {
        SnowflakeClientConfig config =
                SnowflakeClientConfig.builder()
                        .url("https://myorg-myaccount.snowflakecomputing.com")
                        .user("testuser")
                        .role("testrole")
                        .accountId("myorg-myaccount")
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
}
