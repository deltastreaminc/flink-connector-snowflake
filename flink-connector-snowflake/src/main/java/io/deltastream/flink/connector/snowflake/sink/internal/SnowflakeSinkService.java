/*
 * Copyright (c) 2023 DeltaStream, Inc. All rights reserved.
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
