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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeClientConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * This class is the service implementation for managing ingest client and channel for writing
 * Snowflake rows to a db.schema.table in the external service.
 */
@Internal
public class SnowflakeSinkServiceImpl implements SnowflakeSinkService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkServiceImpl.class);

    // downstream configuration
    private final SnowflakeWriterConfig writerConfig;
    private final SnowflakeChannelConfig channelConfig;

    // connections
    private final SnowflakeStreamingIngestClient client;
    // A channel name computed from a unique ingestion name and subtask ID
    private final String channelName;
    // The expected offset to be committed by the Snowpipe channel
    private long offset;
    // The last committed offset that was confirmed by Snowflake
    private long lastCommittedOffset;

    /**
     * 1-1 mapping between client/channel/flink subtask Channel for communicating with the Snowflake
     * ingest APIs Per Snowflake documentation.
     */
    private final SnowflakeStreamingIngestChannel channel;

    // metrics
    private final Counter numRecordsOut;
    private final Counter numRecordsIn;
    private final Counter numRecordsSendError;

    public SnowflakeWriterConfig getWriterConfig() {
        return writerConfig;
    }

    public SnowflakeChannelConfig getChannelConfig() {
        return channelConfig;
    }

    public SnowflakeStreamingIngestClient getClient() {
        return this.client;
    }

    public String getChannelName() {
        return channelName;
    }

    public SnowflakeStreamingIngestChannel getChannel() {
        return channel;
    }

    /**
     * Construct a new sink service to provide APIs to the Snowflake service.
     *
     * @param appId {@link java.lang.String} UID for Flink job
     * @param taskId {@link java.lang.Integer} Flink subtask ID
     * @param clientConfig {@link SnowflakeClientConfig}
     * @param writerConfig {@link SnowflakeWriterConfig}
     * @param channelConfig {@link SnowflakeChannelConfig}
     * @param metricGroup {@link SinkWriterMetricGroup}
     */
    public SnowflakeSinkServiceImpl(
            final String appId,
            final int taskId,
            final SnowflakeClientConfig clientConfig,
            final SnowflakeWriterConfig writerConfig,
            final SnowflakeChannelConfig channelConfig,
            SinkWriterMetricGroup metricGroup) {
        this.writerConfig = Preconditions.checkNotNull(writerConfig, "writerConfig");
        this.channelConfig = Preconditions.checkNotNull(channelConfig, "channelConfig");
        this.channelName = SnowflakeInternalUtils.createClientOrChannelName(appId, taskId);

        // ingest client
        this.client = this.createClientFromConfig(appId, channelConfig, clientConfig);

        // ingest channel
        LOGGER.info(
                "Opening a new streaming channel '{}' for the client '{}'",
                this.getChannelName(),
                this.getClient().getClientName());
        this.channel = Preconditions.checkNotNull(this.openChannelFromConfig());
        this.offset = this.getLatestCommittedOffsetFromChannelStatus();
        this.lastCommittedOffset = this.offset;

        // metrics counters
        final SinkWriterMetricGroup sinkMetricGroup =
                Preconditions.checkNotNull(metricGroup, "metricGroup");
        this.numRecordsIn = sinkMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
        this.numRecordsOut = sinkMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.numRecordsSendError = sinkMetricGroup.getNumRecordsSendErrorsCounter();
    }

    @Override
    public void insert(Map<String, Object> row) throws IOException {
        try {
            this.numRecordsIn.inc();
            this.offset++;
            this.getChannel().appendRow(row, Long.toString(offset));
            LOGGER.debug("Submitted row to channel '{}'", this.getChannelName());

            // check whether there were any errors since the last insert, and fail early if so
            this.checkErrors();
        } catch (SFException e) {
            throw new IOException("Failed to append row with Snowflake sink service", e);
        }
    }

    @Override
    public void flush() throws IOException {

        // trigger flush in the background
        this.flushAsync();

        // await committed offset, and reset channel state, if applicable
        this.alignOffsetAndReset();
    }

    /**
     * Flush the Snowflake ingest channel asynchronously without waiting for the result. The Offset
     * commit will must be handled separately after on-demand flush.
     *
     * @throws IOException on flush failure
     */
    private void flushAsync() throws IOException {

        /*
         * The ingest channel periodically flushes data based on the buffer configuration, and
         * commits all buffered data on close. So when we don't need to guarantee delivery, we
         * skip forceful flush of data.
         */
        if (this.getWriterConfig().getDeliveryGuarantee().equals(DeliveryGuarantee.NONE)) {
            LOGGER.info(
                    "Skipping force flush for channel '{}' due to delivery guarantee of NONE",
                    this.getChannelName());
            return;
        }

        // TODO move to waiter API for flush with channel
        LOGGER.debug("Flushing Snowflake ingest channel '{}'", this.getChannelName());
        final Object flushRes = invoke(this.getChannel(), "initiateFlush");

        // wait for flush, otherwise fail the checkpoint
        if (flushRes == null) {
            LOGGER.debug("To be replaced with void Flush initiation");
        } else if (flushRes instanceof CompletableFuture) {
            try {
                ((CompletableFuture<?>) flushRes).get();
                LOGGER.info("Successfully triggered channel '{}' flush", this.getChannelName());
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Snowflake channel flush did not finish successfully", e);
            }
        } else {
            throw new IOException(
                    String.format(
                            "Snowflake channel flush did not return a handle to wait on: got %s",
                            flushRes.getClass().getSimpleName()));
        }
    }

    /**
     * After flush, align the expected offset with the committed offset from the Snowflake ingest
     * channel. If the offsets do not align, wait until they do. Then, reset the internal state of
     * the channel if needed. Reseting is done by recreating the channel after a threshold of
     * inserted rows is reached.
     */
    private void alignOffsetAndReset() {

        /*
         * After flush has completed, wait until the Snowflake ingest channel offset matches the expected
         * number of records we have just flushed. In this code block, we retry infinitely (with 1-second
         * sleep between tries). If the offset on the channel never catches up to our expected offset,
         * the Flink job will eventually abort the checkpoint after the checkpoint timeout duration has
         * expired.
         */
        int retryCount = 0;
        long committedOffset = this.getLatestCommittedOffsetFromChannelStatus();
        while (committedOffset < this.offset) {

            /*
             * As records are committed during the wait, we incrementally report to metrics to provide
             * real-time visibility into the underlying flush progress.
             * This won't report anything if the flush flow is not used to align offsets, e.g. when delivery
             * guarantee is NONE.
             */
            this.reportNumRecordsOut(committedOffset);

            try {
                LOGGER.info(
                        "Waiting for channel '{}' to commit offset {}/{} (retry #{}, {} records remaining, status {})",
                        this.getChannelName(),
                        committedOffset,
                        this.offset,
                        retryCount,
                        this.offset - committedOffset,
                        this.getChannel().getChannelStatus().getStatusCode());

                //noinspection BusyWait
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOGGER.warn(
                        "Thread sleep interrupted while waiting for Snowflake records to flush");
            }
            retryCount++;
            committedOffset = this.getLatestCommittedOffsetFromChannelStatus();
        }

        /*
         * At this point, the committed offset has caught up to the expected offset after flush,
         * so we can report any remaining committed records to metrics.
         */
        this.reportNumRecordsOut(committedOffset);
    }

    /**
     * Report the number of newly committed records to metrics based on the committed offset from
     * the Snowflake ingest channel. This method is used to incrementally report records committed
     * to Snowflake.
     *
     * @param committedOffset the latest committed offset from the Snowflake ingest channel
     */
    private void reportNumRecordsOut(long committedOffset) {
        final long newlyCommittedRecords = committedOffset - this.lastCommittedOffset;
        if (newlyCommittedRecords > 0) {
            this.numRecordsOut.inc(newlyCommittedRecords);
            this.lastCommittedOffset += newlyCommittedRecords;
        }
    }

    SnowflakeStreamingIngestClient createClientFromConfig(
            final String appId,
            final SnowflakeChannelConfig channelConfig,
            final SnowflakeClientConfig clientConfig) {
        return SnowflakeStreamingIngestClientProvider.createClient(
                appId,
                channelConfig.getDatabaseName(),
                channelConfig.getSchemaName(),
                channelConfig.getTableName(),
                clientConfig);
    }

    /**
     * Open a channel for a {@link SnowflakeWriterConfig}. A channel is opened if this service is
     * creating a new one or recovering form a failure as recommended by the Snowflake service: See:
     * <a
     * href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#exactly-once-delivery-best-practices">Streaming
     * Best Practices</a>
     *
     * @return {@link SnowflakeStreamingIngestChannel}
     */
    SnowflakeStreamingIngestChannel openChannelFromConfig() {
        LOGGER.debug(
                "Opening a '{}' channel for table '{}'",
                this.getChannelName(),
                this.getChannelConfig().getTableName());
        final SnowflakeStreamingIngestChannel channel =
                this.getClient().openChannel(this.getChannelName()).getChannel();
        LOGGER.info(
                "Successfully opened channel '{}' for table '{}'",
                channel.getFullyQualifiedChannelName(),
                this.getChannelConfig().getTableName());
        return channel;
    }

    @Override
    public void close() throws Exception {
        try {
            this.closeChannel();
        } catch (SFException e) {

            // gracefully logging an error to allow the rest of the resources to be cleaned up
            LOGGER.error(
                    "Failed to cleanly close the channel '{}', and some records may not have been committed",
                    this.getChannelName(),
                    e);
        }

        LOGGER.info("Closing Snowflake client '{}'", this.getClient().getClientName());
        IOUtils.closeAll(this.getClient());
        LOGGER.info(
                "Snowflake ingest client '{}' has been successfully closed",
                this.getClient().getClientName());
    }

    /**
     * Synchronously close the channel, which will trigger a commit of all buffered data. If the
     * channel is already closed, this method is a no-op.
     *
     * @throws SFException if the close operation fails, which should be handled by the caller.
     */
    void closeChannel() throws SFException {

        // no-op if channel is already closed
        if (this.getChannel().isClosed()) {
            return;
        }

        try {
            /*
             * attempt to close, and wait for the flush to complete with a timeout of half the
             * max buffer time
             */
            LOGGER.info(
                    "Closing Snowflake ingest channel '{}'", this.getChannel().getChannelName());
            this.getChannel()
                    .close(
                            true,
                            Duration.ofSeconds(
                                    SnowflakeChannelConfig.GLOBAL_API_TIMEOUT_MS_DEFAULT));
            LOGGER.info(
                    "Snowflake ingest channel '{}' has been successfully closed",
                    this.getChannel().getChannelName());
        } catch (TimeoutException e) {
            LOGGER.error(
                    "Failed to cleanly close the Snowflake ingest channel '{}', and some records may not have been committed",
                    this.getChannelName());
        }
    }

    /**
     * Check whether the channel has encountered any issues. Throws a {@link FlinkRuntimeException}
     * if there were issues, making this handling fatal without any retries.
     */
    void checkErrors() throws IOException {
        /*
         * no-op iff:
         *   1) no insert validation or validation had no errors, AND
         *   2) the channel status code is SUCCESS, which means the channel is healthy and able to process more rows
         */
        if (this.getChannel().getChannelStatus().getRowsErrorCount() <= 0
                && this.getChannel().getChannelStatus().getStatusCode().equals("SUCCESS")) {
            return;
        }

        LOGGER.debug(
                "Encountered error on row submission to Snowflake ingest channel '{}'",
                this.getChannelName());
        this.numRecordsSendError.inc(this.getChannel().getChannelStatus().getRowsErrorCount());

        // fatal
        throw new IOException(
                String.format(
                        "Encountered errors while appending rows into Snowflake up to offset %s: code=%s, msg=%s",
                        this.getChannel().getChannelStatus().getLastErrorOffsetTokenUpperBound(),
                        this.getChannel().getChannelStatus().getStatusCode(),
                        this.getChannel().getChannelStatus().getLastErrorMessage()));
    }

    protected long getLatestCommittedOffsetFromChannelStatus() {
        String lastOffsetToken =
                this.getChannel().getChannelStatus().getLatestCommittedOffsetToken();
        try {
            return StringUtils.isEmpty(lastOffsetToken) ? 0 : Long.parseLong(lastOffsetToken);
        } catch (NumberFormatException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The offsetToken '%s' cannot be parsed as a long for channel '%s'",
                            lastOffsetToken, this.getChannelName()),
                    e);
        }
    }

    /**
     * Invoke ingest channel internal method using reflection.
     *
     * @param object {@link java.lang.Object}
     * @param methodName {@link java.lang.String}
     * @param argTypes {@link Class} array
     * @param args {@link java.lang.Object} array
     * @return {@link java.lang.Object} result of the invocation
     */
    // TODO remove
    private static Object invoke(Object object, String methodName) {
        try {
            Method method = object.getClass().getMethod(methodName);
            method.setAccessible(true);
            return method.invoke(object);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible SnowflakeStreamingIngestChannel version", e);
        }
    }
}
