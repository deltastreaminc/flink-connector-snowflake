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

package io.deltastream.flink.connector.snowflake.sink.context;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;

/**
 * Default implementation of {@link SnowflakeSinkContext} to providing minimal context for writing
 * data to the {@link io.deltastream.flink.connector.snowflake.sink.SnowflakeSink}.
 */
@Internal
public class DefaultSnowflakeSinkContext implements SnowflakeSinkContext {

    private final Sink.InitContext initContext;
    private final boolean flushOnCheckpoint;
    private final SnowflakeWriterConfig writerConfig;
    private final String appId;

    public DefaultSnowflakeSinkContext(
            Sink.InitContext initContext, SnowflakeWriterConfig writerConfig, String appId) {
        this.initContext = initContext;
        this.writerConfig = writerConfig;
        this.flushOnCheckpoint =
                !DeliveryGuarantee.NONE.equals(this.writerConfig.getDeliveryGuarantee());
        this.appId = appId;
    }

    @Override
    public Sink.InitContext getInitContext() {
        return this.initContext;
    }

    @Override
    public long processTime() {
        return this.getInitContext().getProcessingTimeService().getCurrentProcessingTime();
    }

    @Override
    public SnowflakeWriterConfig getWriterConfig() {
        return this.writerConfig;
    }

    @Override
    public String getAppId() {
        return this.appId;
    }

    @Override
    public boolean isFlushOnCheckpoint() {
        return this.flushOnCheckpoint;
    }
}
