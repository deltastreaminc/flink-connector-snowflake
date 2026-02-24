package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.deltastream.flink.connector.snowflake.sink.config.ObservabilityConfig;

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

    // ====================================================================
    // Observability configuration constants
    // ====================================================================
    /**
     * Observability configuration keys and defaults for Snowflake Streaming Ingest SDK. See <a
     * href="https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations#environment-variables">Snowflake
     * Environment Variables</a>
     */
    public static final class Observability {
        /** Enable metrics collection for Snowflake Streaming Ingest SDK. */
        public static final String ENABLE_METRICS_KEY = "SS_ENABLE_METRICS";

        public static final boolean ENABLE_METRICS_DEFAULT = false;

        /** Port number for exposing Snowflake SDK metrics. */
        public static final String METRICS_PORT_KEY = "SS_METRICS_PORT";

        public static final int METRICS_PORT_DEFAULT = 50000;

        /** IP address for exposing Snowflake SDK metrics. */
        public static final String METRICS_IP_KEY = "SS_METRICS_IP";

        public static final String METRICS_IP_DEFAULT = "127.0.0.1";

        /** Log level for Snowflake Streaming Ingest SDK. Valid values: info, warn, error. */
        public static final String LOG_LEVEL_KEY = "SS_LOG_LEVEL";

        public static final ObservabilityConfig.LogLevel LOG_LEVEL_DEFAULT =
                ObservabilityConfig.LogLevel.INFO;

        private Observability() {
            // utility class
        }
    }
}
