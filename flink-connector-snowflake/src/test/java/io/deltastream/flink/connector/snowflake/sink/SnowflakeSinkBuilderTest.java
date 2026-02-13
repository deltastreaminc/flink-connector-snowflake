package io.deltastream.flink.connector.snowflake.sink;

import io.deltastream.flink.connector.snowflake.sink.internal.ClientOptions;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class SnowflakeSinkBuilderTest {

    @Test
    public void testConnectionPropsOnBuild() {

        final SnowflakeSink<Map<String, Object>> sink =
                SnowflakeSink.<Map<String, Object>>builder()
                        .url("test-url")
                        .user("test-user")
                        .role("test-role")
                        .accountId("test-accountId")
                        .database("test_db")
                        .schema("test_schema")
                        .table("test_table")
                        .serializationSchema(
                                ((SnowflakeRowSerializationSchema<Map<String, Object>>)
                                        (element, sinkContext) -> element))
                        .build("build-test");

        Assertions.assertThat(sink.getClientConfig().getConnectionProps())
                .containsKeys(
                        ClientOptions.URL.key(),
                        ClientOptions.USER.key(),
                        ClientOptions.ROLE.key());
    }

    @Test
    public void testFailureOnMissingUrlConnectionProperty() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeSink.builder()
                                        .user("test-user")
                                        .role("test-role")
                                        .build("build-test"))
                .hasMessageContaining("Required connection properties documented by Snowflake");
    }

    @Test
    public void testFailureOnMissingUserConnectionProperty() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeSink.builder()
                                        .url("test-url")
                                        .role("test-role")
                                        .build("build-test"))
                .hasMessageContaining("Required connection properties documented by Snowflake");
    }

    @Test
    public void testFailureOnMissingRoleConnectionProperty() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeSink.builder()
                                        .url("test-url")
                                        .user("test-user")
                                        .build("build-test"))
                .hasMessageContaining("Required connection properties documented by Snowflake");
    }

    @Test
    public void testFailureOnInvalidUrlConnectionProperty() {
        Assertions.assertThatThrownBy(() -> SnowflakeSink.builder().url(""))
                .hasMessage("Invalid url");
    }

    @Test
    public void testFailureOnInvalidPrivateKeyConnectionProperty() {
        Assertions.assertThatThrownBy(() -> SnowflakeSink.builder().privateKey(""))
                .hasMessage("Invalid private_key");
    }

    @Test
    public void testFailureOnInvalidAccountIdConnectionProperty() {
        Assertions.assertThatThrownBy(() -> SnowflakeSink.builder().accountId(""))
                .hasMessage("Invalid accountId");
    }

    @Test
    public void testFailureOnMissingPrivateKeyWithPassphrase() {
        Assertions.assertThatThrownBy(
                        () ->
                                SnowflakeSink.builder()
                                        .url("test-url")
                                        .user("test-user")
                                        .role("test-role")
                                        .accountId("test-accountId")
                                        .keyPassphrase("some-passphrase")
                                        .build("build-test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "%s requires %s",
                                ClientOptions.PRIVATE_KEY_PASSPHRASE.key(),
                                ClientOptions.PRIVATE_KEY.key()));
    }

    @Test
    public void testFailureOnMissingPrivateKeyWithEmptyPassphrase() {
        final SnowflakeSinkBuilder<Map<String, Object>> bSink =
                SnowflakeSink.<Map<String, Object>>builder()
                        .url("test-url")
                        .user("test-user")
                        .role("test-role")
                        .accountId("test-accountId")
                        .database("test_db")
                        .schema("test_schema")
                        .table("test_table")
                        .privateKey("some-priv-key")
                        .keyPassphrase("")
                        .serializationSchema(
                                ((SnowflakeRowSerializationSchema<Map<String, Object>>)
                                        (element, sinkContext) -> element));

        final SnowflakeSink<Map<String, Object>> sink = bSink.build("passphrase-test");
        Assertions.assertThat(
                        sink.getClientConfig()
                                .getConnectionProps()
                                .get(ClientOptions.PRIVATE_KEY_PASSPHRASE.key()))
                .isEqualTo("");
    }
}
