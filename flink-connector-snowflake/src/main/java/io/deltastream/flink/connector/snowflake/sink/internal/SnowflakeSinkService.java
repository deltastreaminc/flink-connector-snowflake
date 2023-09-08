package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.util.Map;

/**
 * Class for ingesting data to Snowflake table, and it's responsible for managing the lifecycle of
 * the underlying client for ingesting the data into the external service.
 */
@Internal
public interface SnowflakeSinkService extends AutoCloseable {

    /**
     * Insert a {@link java.util.Map} serialized record to be written.
     *
     * @param row {@link java.util.Map} serialized Snowflake row to insert
     */
    void insert(final Map<String, Object> row) throws IOException;

    /**
     * Flush internal data, if applicable.
     *
     * @throws IOException if data flush failed to write to backend
     */
    void flush() throws IOException;
}
