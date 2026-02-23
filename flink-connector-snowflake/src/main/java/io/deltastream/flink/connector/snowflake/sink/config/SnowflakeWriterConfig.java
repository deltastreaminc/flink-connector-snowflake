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
import java.time.Duration;
import java.util.Objects;

/**
 * This class provides configuration for the {@code SnowflakeSinkWriter} on how to execute data
 * delivery.
 */
@Internal
public final class SnowflakeWriterConfig implements Serializable {

    private static final long serialVersionUID = 1806512982691643793L;

    /** Default timeout for waiting for committed offsets to align. */
    public static final Duration COMMIT_TIMEOUT_DEFAULT = Duration.ofMillis(300000L); // 5 minutes

    private final DeliveryGuarantee deliveryGuarantee;
    private final Duration commitTimeout;

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public Duration getCommitTimeout() {
        return commitTimeout;
    }

    private SnowflakeWriterConfig(SnowflakeWriterConfigBuilder builder) {
        this.deliveryGuarantee = builder.deliveryGuarantee;
        this.commitTimeout = builder.commitTimeout;
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
                && Objects.equals(this.getCommitTimeout(), that.getCommitTimeout());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getDeliveryGuarantee(), this.getCommitTimeout());
    }

    public static SnowflakeWriterConfigBuilder builder() {
        return new SnowflakeWriterConfigBuilder();
    }

    /** Builder for {@link SnowflakeWriterConfig}. */
    @Internal
    public static class SnowflakeWriterConfigBuilder {

        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        private Duration commitTimeout = COMMIT_TIMEOUT_DEFAULT;

        /**
         * Set the {@link DeliveryGuarantee} to provide for writing to Snowflake. Note that
         * EXACTLY_ONCE is not supported by the Snowflake sink, and an exception will be thrown, if
         * it is provided.
         *
         * @param deliveryGuarantee {@link DeliveryGuarantee}
         * @return {@code this}
         */
        public SnowflakeWriterConfigBuilder deliveryGuarantee(
                final DeliveryGuarantee deliveryGuarantee) {
            Preconditions.checkArgument(
                    deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                    "Snowflake sink does not support an EXACTLY_ONCE delivery guarantee");
            this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
            return this;
        }

        /**
         * Set the timeout duration for waiting for committed offsets to align during flush
         * operations. This is the maximum time the sink will wait for Snowflake to confirm that all
         * buffered records have been committed before failing the checkpoint.
         *
         * <p>Default: 5 minutes
         *
         * <p>Note: A value of 0 indicates an infinite timeout, which will be internally converted
         * to a very large timeout duration to avoid potential overflow issues with infinite
         * durations in the underlying Snowflake client.
         *
         * @param commitTimeoutMs timeout milliseconds, must be non-negative, a value of 0 indicates
         *     an infinite timeout
         * @return {@code this}
         */
        public SnowflakeWriterConfigBuilder commitTimeoutMs(final long commitTimeoutMs) {
            Preconditions.checkNotNull(commitTimeoutMs, "commitTimeoutMs cannot be null");
            Preconditions.checkArgument(
                    commitTimeoutMs >= 0, "commitTimeoutMs must be non-negative");

            if (commitTimeoutMs == 0L) {
                this.commitTimeout = Duration.ofMillis(Integer.MAX_VALUE);
            } else {
                this.commitTimeout = Duration.ofMillis(commitTimeoutMs);
            }
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
