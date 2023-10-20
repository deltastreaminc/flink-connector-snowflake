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
