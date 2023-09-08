package io.deltastream.flink.connector.snowflake.sink.internal;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SnowflakeInternalUtilsTest {

    @Test
    void testCreateClientOrChannelNameWithAllParts() {
        Assertions.assertThat(SnowflakeInternalUtils.createClientOrChannelName("prefix", "name", 0))
                .isEqualTo("prefix_name_0");
    }

    @Test
    void testCreateClientOrChannelNameWithoutSomeParts() {
        Assertions.assertThat(SnowflakeInternalUtils.createClientOrChannelName(null, "name", 0))
                .isEqualTo("name_0");
        Assertions.assertThat(SnowflakeInternalUtils.createClientOrChannelName("prefix", "", 0))
                .isEqualTo("prefix_0");
    }

    @Test
    void testInvalidCreateClientOrChannelNameWithoutPrefixOrName() {
        Assertions.assertThatThrownBy(
                        () -> SnowflakeInternalUtils.createClientOrChannelName(null, null, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("One of prefix or name must be set");
    }
}
