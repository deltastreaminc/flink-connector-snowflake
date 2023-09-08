package io.deltastream.flink.connector.snowflake.sink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import io.deltastream.flink.connector.snowflake.sink.context.SnowflakeSinkContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for implementing a serialization schema for serializing {@link T} to {@link
 * java.util.Map} of {@link java.lang.String} to {@link java.lang.Object} schema as documented by
 * the Snowflake service.
 *
 * @param <T> type of the data to be serialized by the implementation of this interface
 */
public interface SnowflakeRowSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before {@link #serialize(Object,
     * SnowflakeSinkContext)}, hence, suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as registering user metrics.
     *
     * @param initContext {@link
     *     org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext}
     *     Contextual information for initialization
     * @param sinkContext {@link SnowflakeSinkContext} Runtime context
     */
    default void open(
            SerializationSchema.InitializationContext initContext, SnowflakeSinkContext sinkContext)
            throws Exception {
        // No-op by default
    }

    /**
     * Serializes an element and returns it as a {@link java.util.Map} of {@link java.lang.String}
     * to {@link java.lang.Object}.
     *
     * @param element generically typed element to be serialized
     * @param sinkContext Runtime context, e.g. subtask ID, etc.
     * @return {@link java.util.Map} of {@link java.lang.String} to {@link java.lang.Object}
     */
    Map<String, Object> serialize(T element, SnowflakeSinkContext sinkContext);
}
