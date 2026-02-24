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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ObservabilityConfig}. */
class ObservabilityConfigTest {

    @Test
    void testDefaultValues() {
        ObservabilityConfig config = ObservabilityConfig.builder().build();

        Assertions.assertFalse(config.isEnableMetrics());
        Assertions.assertEquals(
                ClientOptions.Observability.METRICS_PORT_DEFAULT, config.getMetricsPort());
        Assertions.assertEquals(
                ClientOptions.Observability.METRICS_IP_DEFAULT, config.getMetricsIp());
        Assertions.assertEquals(ObservabilityConfig.LogLevel.INFO, config.getLogLevel());
        Assertions.assertEquals("info", config.getLogLevelValue());
    }

    @Test
    void testEnableMetrics() {
        ObservabilityConfig config = ObservabilityConfig.builder().enableMetrics().build();

        Assertions.assertTrue(config.isEnableMetrics());
    }

    @Test
    void testCustomMetricsPort() {
        ObservabilityConfig config = ObservabilityConfig.builder().metricsPort(8080).build();

        Assertions.assertEquals(8080, config.getMetricsPort());
    }

    @Test
    void testInvalidMetricsPort() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ObservabilityConfig.builder().metricsPort(0).build());

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ObservabilityConfig.builder().metricsPort(70000).build());
    }

    @Test
    void testCustomMetricsIp() {
        ObservabilityConfig config = ObservabilityConfig.builder().metricsIp("0.0.0.0").build();

        Assertions.assertEquals("0.0.0.0", config.getMetricsIp());
    }

    @Test
    void testNullMetricsIp() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> ObservabilityConfig.builder().metricsIp(null).build());
    }

    @Test
    void testCustomLogLevelWithString() {
        ObservabilityConfig config =
                ObservabilityConfig.builder().logLevel(ObservabilityConfig.LogLevel.WARN).build();

        Assertions.assertEquals(ObservabilityConfig.LogLevel.WARN, config.getLogLevel());
        Assertions.assertEquals("warn", config.getLogLevelValue());
    }

    @Test
    void testCustomLogLevelWithEnum() {
        ObservabilityConfig config =
                ObservabilityConfig.builder().logLevel(ObservabilityConfig.LogLevel.ERROR).build();

        Assertions.assertEquals(ObservabilityConfig.LogLevel.ERROR, config.getLogLevel());
        Assertions.assertEquals("error", config.getLogLevelValue());
    }

    @Test
    void testAllConfigOptions() {
        ObservabilityConfig config =
                ObservabilityConfig.builder()
                        .enableMetrics()
                        .metricsPort(9090)
                        .metricsIp("192.168.1.1")
                        .logLevel(ObservabilityConfig.LogLevel.ERROR)
                        .build();

        Assertions.assertTrue(config.isEnableMetrics());
        Assertions.assertEquals(9090, config.getMetricsPort());
        Assertions.assertEquals("192.168.1.1", config.getMetricsIp());
        Assertions.assertEquals(ObservabilityConfig.LogLevel.ERROR, config.getLogLevel());
        Assertions.assertEquals("error", config.getLogLevelValue());
    }

    @Test
    void testBuilderWithConsumer() {
        // Test that the builder can be used with a consumer pattern
        ObservabilityConfig.ObservabilityConfigBuilder builder = ObservabilityConfig.builder();

        // Simulate the consumer pattern used in SnowflakeSinkBuilder
        java.util.function.Consumer<ObservabilityConfig.ObservabilityConfigBuilder> configurer =
                obs ->
                        obs.enableMetrics()
                                .metricsPort(8888)
                                .metricsIp("10.0.0.1")
                                .logLevel(ObservabilityConfig.LogLevel.WARN);

        configurer.accept(builder);
        ObservabilityConfig config = builder.build();

        Assertions.assertTrue(config.isEnableMetrics());
        Assertions.assertEquals(8888, config.getMetricsPort());
        Assertions.assertEquals("10.0.0.1", config.getMetricsIp());
        Assertions.assertEquals(ObservabilityConfig.LogLevel.WARN, config.getLogLevel());
        Assertions.assertEquals("warn", config.getLogLevelValue());
    }

    @Test
    void testLogLevelEnumValues() {
        // Test all enum values
        Assertions.assertEquals("info", ObservabilityConfig.LogLevel.INFO.getValue());
        Assertions.assertEquals("warn", ObservabilityConfig.LogLevel.WARN.getValue());
        Assertions.assertEquals("error", ObservabilityConfig.LogLevel.ERROR.getValue());
    }

    @Test
    void testLogLevelFromString() {
        Assertions.assertEquals(
                ObservabilityConfig.LogLevel.INFO, ObservabilityConfig.LogLevel.fromString("info"));
        Assertions.assertEquals(
                ObservabilityConfig.LogLevel.WARN, ObservabilityConfig.LogLevel.fromString("WARN"));
        Assertions.assertEquals(
                ObservabilityConfig.LogLevel.ERROR,
                ObservabilityConfig.LogLevel.fromString("ErRoR"));
    }

    @Test
    void testLogLevelFromStringInvalid() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ObservabilityConfig.LogLevel.fromString("debug"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ObservabilityConfig.LogLevel.fromString(null));
    }
}
