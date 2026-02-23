package io.deltastream.flink.connector.snowflake.sink.config;

import org.apache.flink.connector.base.DeliveryGuarantee;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SnowflakeWriterConfigTest {

    @Test
    public void testDefaultWriterConfig() {
        final SnowflakeWriterConfig config = SnowflakeWriterConfig.builder().build();
        Assertions.assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, config.getDeliveryGuarantee());
        Assertions.assertEquals(
                SnowflakeWriterConfig.COMMIT_TIMEOUT_DEFAULT, config.getCommitTimeout());
    }

    @Test
    public void testCustomDeliveryGuarantee() {
        final SnowflakeWriterConfig config =
                SnowflakeWriterConfig.builder().deliveryGuarantee(DeliveryGuarantee.NONE).build();
        Assertions.assertEquals(DeliveryGuarantee.NONE, config.getDeliveryGuarantee());
    }

    @Test
    public void testExactlyOnceNotSupported() {
        final IllegalArgumentException iae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SnowflakeWriterConfig.builder()
                                        .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE));
        Assertions.assertTrue(
                iae.getMessage()
                        .contains(
                                "Snowflake sink does not support an EXACTLY_ONCE delivery guarantee"));
    }

    @Test
    public void testCustomCommitTimeout() {
        final long customTimeoutMs = 300_000L; // 5 minutes
        final SnowflakeWriterConfig config =
                SnowflakeWriterConfig.builder().commitTimeoutMs(customTimeoutMs).build();
        Assertions.assertEquals(customTimeoutMs, config.getCommitTimeout().toMillis());
    }

    @Test
    public void testNegativeCommitTimeout() {
        final IllegalArgumentException iae =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> SnowflakeWriterConfig.builder().commitTimeoutMs(-1));
        Assertions.assertEquals("commitTimeoutMs must be non-negative", iae.getMessage());
    }

    @Test
    public void testZeroCommitTimeout() {
        final SnowflakeWriterConfig config =
                SnowflakeWriterConfig.builder().commitTimeoutMs(0L).build();
        Assertions.assertEquals(Integer.MAX_VALUE, config.getCommitTimeout().toMillis());
    }

    @Test
    public void testAllCustomConfigurations() {
        final long customTimeoutMs = 20_0000L; // 20 minutes
        final SnowflakeWriterConfig config =
                SnowflakeWriterConfig.builder()
                        .deliveryGuarantee(DeliveryGuarantee.NONE)
                        .commitTimeoutMs(customTimeoutMs)
                        .build();

        Assertions.assertEquals(DeliveryGuarantee.NONE, config.getDeliveryGuarantee());
        Assertions.assertEquals(customTimeoutMs, config.getCommitTimeout().toMillis());
    }

    @Test
    public void testEqualsAndHashCode() {
        final SnowflakeWriterConfig config1 =
                SnowflakeWriterConfig.builder().commitTimeoutMs(100_000L).build();

        final SnowflakeWriterConfig config2 =
                SnowflakeWriterConfig.builder().commitTimeoutMs(100_000L).build();

        Assertions.assertEquals(config1, config2);
        Assertions.assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEquals() {
        final SnowflakeWriterConfig config1 =
                SnowflakeWriterConfig.builder().commitTimeoutMs(60_000L).build();

        final SnowflakeWriterConfig config2 =
                SnowflakeWriterConfig.builder().commitTimeoutMs(120_000L).build();

        Assertions.assertNotEquals(config1, config2);
    }
}
