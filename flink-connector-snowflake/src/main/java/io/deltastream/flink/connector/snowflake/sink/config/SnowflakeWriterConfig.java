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

package io.deltastream.flink.connector.snowflake.sink.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class provides configuration for the {@code SnowflakeSinkWriter} on how to execute data
 * delivery.
 */
@Internal
public final class SnowflakeWriterConfig implements Serializable {

    private static final long serialVersionUID = 1806512982691643793L;

    // buffer flush minimum and default
    public static final long BUFFER_FLUSH_TIME_MILLISECONDS_DEFAULT = 1000;
    public static final long BUFFER_FLUSH_TIME_MILLISECONDS_MIN = 10;

    private final DeliveryGuarantee deliveryGuarantee;
    private final long maxBufferTimeMs;

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public long getMaxBufferTimeMs() {
        return maxBufferTimeMs;
    }

    private SnowflakeWriterConfig(SnowflakeWriterConfigBuilder builder) {
        this.deliveryGuarantee = builder.deliveryGuarantee;
        this.maxBufferTimeMs = builder.maxBufferTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnowflakeWriterConfig that = (SnowflakeWriterConfig) o;
        return Objects.equals(this.getDeliveryGuarantee(), that.getDeliveryGuarantee())
                && Objects.equals(this.getMaxBufferTimeMs(), that.getMaxBufferTimeMs());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getDeliveryGuarantee(), this.getMaxBufferTimeMs());
    }

    public static SnowflakeWriterConfigBuilder builder() {
        return new SnowflakeWriterConfigBuilder();
    }

    /** Builder for {@link SnowflakeWriterConfig}. */
    @Internal
    public static class SnowflakeWriterConfigBuilder {

        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        private long maxBufferTimeMs = BUFFER_FLUSH_TIME_MILLISECONDS_DEFAULT;

        public SnowflakeWriterConfigBuilder deliveryGuarantee(
                final DeliveryGuarantee deliveryGuarantee) {
            Preconditions.checkArgument(
                    deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                    "Snowflake sink does not support an EXACTLY_ONCE delivery guarantee");
            this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
            return this;
        }

        public SnowflakeWriterConfigBuilder maxBufferTimeMs(final long maxBufferTimeMs) {
            Preconditions.checkArgument(
                    maxBufferTimeMs >= BUFFER_FLUSH_TIME_MILLISECONDS_MIN,
                    "Buffer must be flushed at least every %s milliseconds",
                    BUFFER_FLUSH_TIME_MILLISECONDS_MIN);
            this.maxBufferTimeMs = maxBufferTimeMs;
            return this;
        }

        /**
         * Build a {@link SnowflakeWriterConfig} from user-provided writer configurations.
         *
         * @return {@link SnowflakeWriterConfig}
         */
        public SnowflakeWriterConfig build() {
            return new SnowflakeWriterConfig(this);
        }

        private SnowflakeWriterConfigBuilder() {}
    }
}
