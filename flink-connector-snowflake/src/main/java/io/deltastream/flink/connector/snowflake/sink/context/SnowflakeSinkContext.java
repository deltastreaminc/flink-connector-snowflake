package io.deltastream.flink.connector.snowflake.sink.context;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;

/**
 * This context provides information for {@link
 * io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema}.
 */
@PublicEvolving
public interface SnowflakeSinkContext {

    /** Get the current init context in sink. */
    Sink.InitContext getInitContext();

    /** Get the current process time in Flink. */
    long processTime();

    /**
     * Get the write options for {@link
     * io.deltastream.flink.connector.snowflake.sink.SnowflakeSink}.
     */
    SnowflakeWriterConfig getWriterConfig();

    String getAppId();

    boolean isFlushOnCheckpoint();
}
