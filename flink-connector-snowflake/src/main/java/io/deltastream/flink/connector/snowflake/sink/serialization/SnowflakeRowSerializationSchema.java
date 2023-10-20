/*
 * Copyright (c) 2023 DeltaStream Inc. All rights reserved.
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
