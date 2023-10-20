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
package io.deltastream.flink.connector.snowflake.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeWriterConfig;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

    /**
     * 1-1 mapping between client/channel/flink subtask Channel for communicating with the Snowflake
     * ingest APIs Per Snowflake documentation, the channel should be reopened on failure recovery.
     */
    private SnowflakeStreamingIngestChannel channel;

    // metrics
    private final Counter numRecordsSendCounter;
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
     * @param connectionConfig {@link java.util.Properties} Snowflake connection settings
     * @param writerConfig {@link SnowflakeWriterConfig}
     * @param channelConfig {@link SnowflakeChannelConfig}
     * @param metricGroup {@link SinkWriterMetricGroup}
     */
    public SnowflakeSinkServiceImpl(
            final String appId,
            final int taskId,
            final Properties connectionConfig,
            final SnowflakeWriterConfig writerConfig,
            final SnowflakeChannelConfig channelConfig,
            SinkWriterMetricGroup metricGroup) {
        this.writerConfig = Preconditions.checkNotNull(writerConfig, "writerConfig");
        this.channelConfig = Preconditions.checkNotNull(channelConfig, "channelConfig");

        // ingest client
        this.client = this.createClientFromConfig(appId, connectionConfig);

        // ingest channel
        this.channelName = SnowflakeInternalUtils.createClientOrChannelName(null, appId, taskId);
        LOGGER.info(
                "Opening a new ingest channel '{}' for the client '{}'",
                this.getChannelName(),
                this.getClient().getName());
        this.channel = Preconditions.checkNotNull(this.openChannelFromConfig());

        // metrics counters
        final SinkWriterMetricGroup sinkMetricGroup =
                Preconditions.checkNotNull(metricGroup, "metricGroup");
        this.numRecordsSendCounter = sinkMetricGroup.getNumRecordsSendCounter();
        this.numRecordsSendError = sinkMetricGroup.getNumRecordsSendErrorsCounter();
    }

    @Override
    public void insert(Map<String, Object> row) throws IOException {

        // send row to ingest channel in a fallback
        InsertValidationResponse response = this.insertRowWithFallback(row);
        this.numRecordsSendCounter.inc();
        LOGGER.debug("Submitted row to Snowflake ingest channel '{}'", this.getChannelName());

        // handle possible errors
        if (response.hasErrors()) {
            LOGGER.debug(
                    "Encountered error on row submission to Snowflake ingest channel '{}'",
                    this.getChannelName());
            this.numRecordsSendError.inc(response.getErrorRowCount());
            this.handleInsertRowsErrors(response.getInsertErrors());
        }
    }

    @Override
    public void flush() {

        /*
         * The ingest channel periodically flushes data based on the buffer configuration, and
         * commits all buffered data on close. So when we don't need to guarantee delivery, we
         * skip forceful flush of data.
         */
        if (this.getWriterConfig().getDeliveryGuarantee().equals(DeliveryGuarantee.NONE)) {
            LOGGER.info(
                    "Skipping force flush for Snowflake ingest channel '{}' for delivery guarantee NONE",
                    this.getChannelName());
            return;
        }

        LOGGER.info("Flushing Snowflake ingest channel '{}'", this.getChannelName());
        invoke(
                this.getChannel(),
                "flush",
                Lists.newArrayList(boolean.class).toArray(Class<?>[]::new),
                Lists.newArrayList(false).toArray());
    }

    SnowflakeStreamingIngestClient createClientFromConfig(
            final String appId, final Properties connectionConfig) {
        return SnowflakeStreamingIngestClientProvider.createClient(
                appId, connectionConfig, this.getWriterConfig());
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
    private SnowflakeStreamingIngestChannel openChannelFromConfig() {
        OpenChannelRequest channelRequest =
                OpenChannelRequest.builder(this.getChannelName())
                        .setDBName(this.getChannelConfig().getDatabaseName())
                        .setSchemaName(this.getChannelConfig().getSchemaName())
                        .setTableName(this.getChannelConfig().getTableName())
                        .setOnErrorOption(this.getChannelConfig().getOnErrorOption())
                        .build();
        LOGGER.debug(
                "Opening a '{}' channel for table '{}'",
                this.getChannelName(),
                this.getChannelConfig().getTableName());
        final SnowflakeStreamingIngestChannel channel =
                this.getClient().openChannel(channelRequest);
        LOGGER.info(
                "Successfully opened channel '{}' for table '{}'",
                this.getChannelName(),
                this.getChannelConfig().getTableName());
        return channel;
    }

    @Override
    public void close() throws Exception {
        if (!this.getChannel().isClosed()) {
            LOGGER.info("Closing Snowflake ingest channel '{}'", this.getChannel().getName());

            // attempt to close (and it commits buffered data)
            try {
                this.getChannel().close().get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error(
                        "Failed to cleanly close the Snowflake ingest channel '{}'",
                        this.getChannelName());
            }
            LOGGER.info(
                    "Snowflake ingest channel '{}' has been successfully closed",
                    this.getChannel().getName());
        }

        LOGGER.info("Closing Snowflake ingest client '{}'", this.getClient().getName());
        IOUtils.closeAll(this.getClient());
        LOGGER.info(
                "Snowflake ingest client '{}' has been successfully closed",
                this.getClient().getName());
    }

    /**
     * Handle errors when {@link InsertValidationResponse} encounters issues. Throws a {@link
     * FlinkRuntimeException} if there were issues, making this handling fatal without any retries.
     *
     * @param errors {@link List (InsertValidationResponse.InsertError)}
     */
    private void handleInsertRowsErrors(List<InsertValidationResponse.InsertError> errors)
            throws IOException {

        // no-op
        if (errors.isEmpty()) {
            return;
        }

        // fatal
        throw new IOException(
                String.format(
                        "Encountered errors while ingesting rows into Snowflake: %s",
                        errors.get(0).getException().getMessage()),
                errors.get(0).getException());
    }

    /**
     * Reopen the ingestion channel on {@link SFException} using {@link dev.failsafe.Fallback}.
     *
     * @param row {@link java.util.Map} to send to Snowflake for eventual flush
     * @return {@link InsertValidationResponse}
     */
    private InsertValidationResponse insertRowWithFallback(final Map<String, Object> row) {
        Fallback<Object> reopenChannelFallbackExecutorForInsertRows =
                Fallback.builder(
                                attempt -> {
                                    this.ingestionFallbackSupplier(attempt.getLastException());
                                })
                        .handle(SFException.class)
                        .onFailedAttempt(
                                event ->
                                        LOGGER.warn(
                                                "Failed to send row to ingest channel",
                                                event.getLastException()))
                        .onFailure(
                                event ->
                                        LOGGER.error(
                                                String.format(
                                                        "[INSERT_ROW_FALLBACK] Failed to re-open channel '%s'",
                                                        this.getChannelName()),
                                                event.getException()))
                        .build();

        return Failsafe.with(reopenChannelFallbackExecutorForInsertRows)
                .get(() -> this.channel.insertRow(row, null));
    }

    /**
     * Fail on any errors that may have happened during ingestion to the Snowflake service.
     *
     * @param throwable {@link Throwable}
     */
    private void ingestionFallbackSupplier(final Throwable throwable) {
        LOGGER.warn(
                "[INSERT_ROWS_FALLBACK] Failed to insert row with channel '{}'. Exiting with error",
                this.getChannelName());
        throw new RuntimeException(throwable);
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
    private static Object invoke(
            Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible SnowflakeStreamingIngestChannel version", e);
        }
    }
}
