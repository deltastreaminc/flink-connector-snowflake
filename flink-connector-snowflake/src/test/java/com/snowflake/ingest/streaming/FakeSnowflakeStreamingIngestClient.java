package com.snowflake.ingest.streaming;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake implementation of {@link com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient}
 * which stores state in memory. This implementation is used for testing and works together with
 * {@link FakeSnowflakeStreamingIngestChannel} for simulating ingest-sdk.
 *
 * <p>Note that this implementation is not thread safe and some functionality may be missing.
 */
public class FakeSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {

    private final String name;
    private boolean closed;
    private final ConcurrentHashMap<String, FakeSnowflakeStreamingIngestChannel> channelCache =
            new ConcurrentHashMap<>();
    private final String dbName;
    private final String schemaName;
    private final String tableName;

    public FakeSnowflakeStreamingIngestClient(
            String name, String dbName, String schemaName, String tableName) {
        this.name = name;
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public ConcurrentHashMap<String, FakeSnowflakeStreamingIngestChannel> getChannelCache() {
        return channelCache;
    }

    @Override
    public OpenChannelResult openChannel(String channelName) {
        SnowflakeStreamingIngestChannel channel =
                channelCache.computeIfAbsent(
                        channelName,
                        (key) ->
                                new FakeSnowflakeStreamingIngestChannel(
                                        channelName, dbName, schemaName, tableName));
        return new OpenChannelResult(channel, null);
    }

    @Override
    public OpenChannelResult openChannel(String channelName, String offsetToken) {
        // For the fake implementation, we ignore the offsetToken parameter
        return openChannel(channelName);
    }

    @Override
    public void dropChannel(String channelName) {
        //noinspection resource
        channelCache.remove(channelName);
    }

    @Override
    public String getClientName() {
        return name;
    }

    @Override
    public String getPipeName() {
        return name + "_pipe";
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public CompletableFuture<Void> waitForFlush(Duration timeoutDuration) {
        // Simulate waiting for all channels to flush their data
        // In a fake implementation, we immediately return a completed future
        // since there's no actual network I/O happening
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public ChannelStatusBatch getChannelStatus(List<String> channelNames) {
        // The ChannelStatus and ChannelStatusBatch classes from the Snowflake SDK
        // need to be properly instantiated based on the actual SDK API
        // For now, returning a simple batch with basic status information

        // Create a status batch with channel statuses
        Map<String, ChannelStatus> channelStatuses = new HashMap<>();

        for (String channelName : channelNames) {
            FakeSnowflakeStreamingIngestChannel channel = channelCache.get(channelName);
            if (channel != null && !channel.isClosed()) {
                // Channel exists and is open - create a valid status
                // The actual constructor parameters depend on the Snowflake SDK version
                channelStatuses.put(channelName, channel.createValidChannelStatus("3"));
            } else if (channel != null && channel.isClosed()) {
                // Channel exists but is closed
                channelStatuses.put(channelName, channel.createClosedChannelStatus());
            } else {
                // Channel doesn't exist
                channelStatuses.put(channelName, channel.createNotFoundChannelStatus());
            }
        }

        return new ChannelStatusBatch(channelStatuses);
    }

    @Override
    public String getDBName() {
        return this.dbName;
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    @Override
    public Map<String, String> getLatestCommittedOffsetTokens(List<String> channelNames) {
        Map<String, String> offsetTokens = new HashMap<>();
        channelNames.forEach(
                name ->
                        offsetTokens.put(
                                name, channelCache.get(name).getLatestCommittedOffsetToken()));
        return offsetTokens;
    }

    @Override
    public void initiateFlush() {
        // No-op for fake implementation
    }

    @Override
    public void close() {
        this.close(true, Duration.ofSeconds(5));
    }

    @Override
    public CompletableFuture<Void> close(boolean flush, Duration timeout) {
        closed = true;
        return CompletableFuture.completedFuture(null);
    }
}
