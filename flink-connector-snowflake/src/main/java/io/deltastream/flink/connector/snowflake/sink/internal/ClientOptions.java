package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@Internal
public class ClientOptions {

    public static final ConfigOption<String> USER =
            ConfigOptions.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("User sign-in name for the Snowflake accountId");

    public static final ConfigOption<String> ACCOUNT_ID =
            ConfigOptions.key("account")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Snowflake accountId identifier; for example, xy12345");

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URL for accessing your Snowflake accountId, including your accountId identifier."
                                    + " The protocol (https://) and port number are optional");

    public static final ConfigOption<String> ROLE =
            ConfigOptions.key("role")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Access control role to use for the session after connecting to Snowflake");

    public static final ConfigOption<String> PRIVATE_KEY =
            ConfigOptions.key("private_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Private key content that is used to authenticate the user."
                                    + " Include only the key content; no header, footer, or line breaks");

    public static final ConfigOption<String> PRIVATE_KEY_PASSPHRASE =
            ConfigOptions.key("private_key_passphrase")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Private key content that is used to authenticate the user."
                                    + " Include only the key content; no header, footer, or line breaks");
}
