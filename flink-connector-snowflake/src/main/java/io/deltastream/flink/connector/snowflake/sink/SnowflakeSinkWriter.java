
package io.deltastream.flink.connector.snowflake.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import io.deltastream.flink.connector.snowflake.sink.config.SnowflakeChannelConfig;
import io.deltastream.flink.connector.snowflake.sink.context.SnowflakeSinkContext;
import io.deltastream.flink.connector.snowflake.sink.internal.SnowflakeSinkService;
import io.deltastream.flink.connector.snowflake.sink.internal.SnowflakeSinkServiceImpl;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;
import net.snowflake.ingest.utils.SFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * This class is responsible to write records to a Snowflake Table and to handle {@link
 * org.apache.flink.connector.base.DeliveryGuarantee#NONE} and {@link
 * org.apache.flink.connector.base.DeliveryGuarantee#AT_LEAST_ONCE} delivery guarantees.
 *
 * <p>This writer may only do one of write, flush, or close at once, where all APIs eventually write
 * produced data to Snowflake.
 *
 * @param <IN> The type of input elements.
 */
class SnowflakeSinkWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkWriter.class);

    // services
    private final SnowflakeSinkService sinkService;
    private final SnowflakeRowSerializationSchema<IN> serializationSchema;

    // internal states
    private boolean checkpointInProgress = false;
    private final SnowflakeSinkContext sinkContext;

    SnowflakeSinkWriter(
            final SnowflakeSinkContext sinkContext,
            final Properties connectionConfigs,
            final SnowflakeChannelConfig channelConfig,
            SnowflakeRowSerializationSchema<IN> serializationSchema) {

        this.sinkContext = Preconditions.checkNotNull(sinkContext, "sinkContext");

        // open serialization
        try {
            serializationSchema.open(
                    this.sinkContext.getInitContext().asSerializationSchemaInitializationContext(),
                    this.sinkContext);
            this.serializationSchema = serializationSchema;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to open the provided serialization schema %s",
                            serializationSchema.getClass().getName()),
                    e);
        }

        // sink service
        try {
            this.sinkService =
                    new SnowflakeSinkServiceImpl(
                            this.sinkContext.getAppId(),
                            this.sinkContext.getInitContext().getSubtaskId(),
                            connectionConfigs,
                            this.sinkContext.getWriterConfig(),
                            channelConfig,
                            this.sinkContext.getInitContext().metricGroup());
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @VisibleForTesting
    SnowflakeSinkWriter(
            final SnowflakeSinkContext sinkContext,
            final SnowflakeSinkService sinkService,
            SnowflakeRowSerializationSchema<IN> serializationSchema) {

        this.sinkContext = sinkContext;
        this.serializationSchema = serializationSchema;
        this.sinkService = sinkService;
    }

    @Override
    public synchronized void write(IN element, Context context)
            throws IOException, InterruptedException {

        // wait for checkpoint to finish
        while (checkpointInProgress) {
            this.sinkContext.getInitContext().getMailboxExecutor().yield();
        }

        /*
         * Send to the service for eventual write
         * This may flush based on SnowflakeSinkService.SnowflakeWriterConfig
         */
        try {
            this.sinkService.insert(this.serializationSchema.serialize(element, sinkContext));
        } catch (SFException e) {
            LOGGER.error("Failed to insert row with Snowflake sink service", e);
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        LOGGER.debug(
                "Sink writer flush was triggered [endOfInput={}, flushOnCheckpoint={}]",
                endOfInput,
                this.sinkContext.isFlushOnCheckpoint());

        this.checkpointInProgress = true;
        if (this.sinkContext.isFlushOnCheckpoint() || endOfInput) {
            this.sinkService.flush();
        }
        this.checkpointInProgress = false;
    }

    @Override
    public synchronized void close() throws Exception {
        /*
         * Underlying Snowflake SnowflakeStreamingIngestChannel.close() promises full commit before closing, so there
         * isn't a need for flushing previously buffered data when closing the sink service
         */
        IOUtils.closeAll(this.sinkService);
    }
}
