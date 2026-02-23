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
import java.util.Map;
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
            LOGGER.debug(
                    "Submitted row to channel '{}'",
                    this.getChannel().getFullyQualifiedChannelName());

            // check whether there were any errors since the last insert, and fail early if so
            this.checkErrors();
        } catch (SFException e) {
            throw new IOException("Failed to append row with Snowflake sink service", e);
        }
    }

    @Override
    public void flush() throws IOException {
        /*
         * The ingest channel periodically flushes rows, and commits all buffered data on close.
         * When we don't need to guarantee delivery, we skip on-demand flush.
         */
        if (this.getWriterConfig().getDeliveryGuarantee().equals(DeliveryGuarantee.NONE)) {
            LOGGER.info(
                    "Skipping committed offset alignment for channel '{}' due to delivery guarantee of NONE",
                    this.getChannel().getFullyQualifiedChannelName());
            return;
        }

        // trigger a flush
        this.flushAsync();

        // await committed offset, and reset channel state, if applicable
        this.alignOffsetAndReset();
    }

    /**
     * Flush and wait the Snowflake ingest channel to successfully execute flushing all pending
     * rows. This method does not wait for all rows to be committed on the server side and that must
     * be handled separately after channel flush has completed.
     */
    private void flushAsync() throws IOException {
        LOGGER.debug(
                "Flushing Snowflake ingest channel '{}'",
                this.getChannel().getFullyQualifiedChannelName());
        try {

            // wait for the channel to flush all pending rows
            this.getChannel().waitForFlush(null).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Snowflake channel flush did not finish successfully", e);
        }
    }

    /**
     * Attempt to align the expected offset with the committed offset from the Snowflake ingest
     * channel, and wait until the offsets align. Then, reset the internal state of the channel if
     * needed. Resetting is done by recreating the channel after a threshold of inserted rows is
     * reached.
     */
    private void alignOffsetAndReset() throws IOException {
        try {
            /*
             * Wait until the channel's offset passes the expected offset for records that were flushed.
             * If the offset on the channel never catches up to our expected offset, the Flink job will
             * eventually abort the checkpoint after the checkpoint timeout duration has expired.
             */
            this.getChannel()
                    .waitForCommit(
                            serverOffset -> {
                                boolean hasCaughtUp = true;
                                long committedOffset = this.parseOffsetToken(serverOffset);

                                // still waiting for the appended rows to be committed
                                if (committedOffset < this.offset) {
                                    hasCaughtUp = false;

                                    LOGGER.info(
                                            "Waiting for channel '{}' to commit offset {}/{} ({} records remaining, status {})",
                                            this.getChannel().getFullyQualifiedChannelName(),
                                            committedOffset,
                                            this.offset,
                                            this.offset - committedOffset,
                                            this.getChannel().getChannelStatus().getStatusCode());
                                } // otherwise, all records are committed on the server

                                /*
                                 * As records are committed during the wait, we incrementally report to metrics to provide
                                 * real-time visibility into the underlying flush progress, until all records are committed,
                                 * and the committed offset had caught up to the expected offset at append.
                                 * This won't report anything if the flush flow is not used to align offsets, e.g. when delivery
                                 * guarantee is NONE.
                                 */
                                this.reportNumRecordsOut(committedOffset);
                                return hasCaughtUp;
                            },
                            this.getWriterConfig().getCommitTimeout())
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to align Snowflake channel offset", e);
        }
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
                    this.getChannel().getFullyQualifiedChannelName(),
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
            // attempt to close, and wait for the flush to complete
            LOGGER.info(
                    "Closing Snowflake ingest channel '{}'",
                    this.getChannel().getFullyQualifiedChannelName());
            this.getChannel().close(true, this.getChannelConfig().getChannelCloseTimeout());
            LOGGER.info(
                    "Snowflake ingest channel '{}' has been successfully closed",
                    this.getChannel().getFullyQualifiedChannelName());
        } catch (TimeoutException e) {
            LOGGER.error(
                    "Failed to cleanly close the Snowflake ingest channel '{}', and some records may not have been committed",
                    this.getChannel().getFullyQualifiedChannelName());
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
                this.getChannel().getFullyQualifiedChannelName());
        this.numRecordsSendError.inc(this.getChannel().getChannelStatus().getRowsErrorCount());

        // fatal
        throw new IOException(
                String.format(
                        "Encountered errors while appending rows into Snowflake up to offset %s: code=%s, msg=%s",
                        this.getChannel().getChannelStatus().getLastErrorOffsetTokenUpperBound(),
                        this.getChannel().getChannelStatus().getStatusCode(),
                        this.getChannel().getChannelStatus().getLastErrorMessage()));
    }

    /**
     * Get the last reported committed offset for the channel parsed as long. Throws {@link
     * FlinkRuntimeException}, if not parsable.
     *
     * @return {@link java.lang.Long}
     */
    protected long getLatestCommittedOffsetFromChannelStatus() {
        return this.parseOffsetToken(
                this.getChannel().getChannelStatus().getLatestCommittedOffsetToken());
    }

    /**
     * Given a record offset token, parse as long and throw a {@link FlinkRuntimeException}, if not
     * parsable.
     *
     * @param offsetToken {@link java.lang.String}
     * @return {@link java.lang.Long}
     */
    private long parseOffsetToken(String offsetToken) {
        try {
            return StringUtils.isEmpty(offsetToken) ? 0 : Long.parseLong(offsetToken);
        } catch (NumberFormatException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The offsetToken '%s' cannot be parsed as a long for channel '%s'",
                            offsetToken, this.getChannel().getFullyQualifiedChannelName()),
                    e);
        }
    }
}
