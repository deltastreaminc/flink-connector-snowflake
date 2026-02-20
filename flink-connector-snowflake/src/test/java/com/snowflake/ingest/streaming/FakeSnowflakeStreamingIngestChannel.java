package com.snowflake.ingest.streaming;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestChannel} which stores state in memory. This
 * implementation is used for testing and is able to keep state between calls: so inserting rows
 * through {@link SnowflakeStreamingIngestChannel#appendRow(Map, String)} will update the {@link
 * SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}.
 *
 * <p>Note that this implementation is not thread safe and some functionality may be missing.
 */
public class FakeSnowflakeStreamingIngestChannel implements SnowflakeStreamingIngestChannel {
    private final String name;
    private final String dbName;
    private final String schemaName;
    private final String tableName;
    private final String fullyQualifiedChannelName;
    private final String fullyQualifiedPipeName;
    private boolean closed;
    private String offsetToken;

    private final List<Map<String, Object>> rows = new LinkedList<>();

    public FakeSnowflakeStreamingIngestChannel(
            String name, String dbName, String schemaName, String tableName) {
        this.name = name;
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fullyQualifiedChannelName =
                String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name);
        this.fullyQualifiedPipeName = String.format("%s.%s.%s_pipe", dbName, schemaName, tableName);
    }

    @Override
    public String getPipeName() {
        return String.format("%s_pipe", tableName);
    }

    @Override
    public String getDBName() {
        return dbName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getFullyQualifiedPipeName() {
        return fullyQualifiedPipeName;
    }

    @Override
    public String getFullyQualifiedChannelName() {
        return fullyQualifiedChannelName;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String getChannelName() {
        return this.name;
    }

    @Override
    public void close() {
        this.close(true, Duration.ofSeconds(5));
    }

    @Override
    public void close(boolean flush, Duration timeout) {
        closed = true;
    }

    @Override
    public void appendRow(Map<String, Object> row, String offsetToken) {
        this.appendRows(List.of(row), null, offsetToken);
    }

    @Override
    public void appendRows(
            Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {
        final List<Map<String, Object>> rowsCopy = new LinkedList<>();
        rows.forEach(r -> rowsCopy.add(new LinkedHashMap<>(r)));
        this.rows.addAll(rowsCopy);
        this.offsetToken = endOffsetToken;
    }

    @Override
    public String getLatestCommittedOffsetToken() {
        return offsetToken;
    }

    @Override
    public ChannelStatus getChannelStatus() {

        // Parse offsetToken to long if present, or use 0L
        long offsetTokenValue = 0L;
        if (offsetToken != null) {
            try {
                offsetTokenValue = Long.parseLong(offsetToken);
            } catch (NumberFormatException e) {
                // If parsing fails, keep 0L
            }
        }
        return createValidChannelStatus(String.valueOf(offsetTokenValue));
    }

    @Override
    public CompletableFuture<Void> waitForCommit(
            Predicate<String> tokenChecker, Duration timeoutDuration) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> waitForFlush(Duration timeoutDuration) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void initiateFlush() {
        // No-op for the fake implementation
    }

    protected ChannelStatus createValidChannelStatus(String offsetToken) {
        // Create a channel status for a valid, open channel
        // ChannelStatus constructor signature from Snowflake SDK:
        // (channelName, dbName, schemaName, tableName, clientName, pipeName,
        //  createdOn, offsetTokenAtLastCommit, rowCountAtLastCommit, rowSequencerAtLastCommit,
        //  startOffsetToken, endOffsetToken, latestCommitTime, latestHeartbeatTime,
        // latestFlushTime)
        return new ChannelStatus(
                dbName,
                schemaName,
                tableName + "_pipe",
                name,
                "3",
                offsetToken,
                java.time.Instant.now(),
                rows.size(),
                0L,
                0L,
                offsetToken,
                null,
                null,
                Duration.ofSeconds(1),
                java.time.Instant.now() // latestFlushTime
                );
    }

    protected ChannelStatus createClosedChannelStatus() {
        return new ChannelStatus(
                name,
                dbName,
                schemaName,
                tableName,
                name,
                "10",
                java.time.Instant.now(),
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null);
    }

    protected ChannelStatus createNotFoundChannelStatus() {
        // For not found channels, use empty/default values
        return new ChannelStatus(
                name,
                dbName,
                schemaName,
                tableName,
                name,
                "5",
                java.time.Instant.now(),
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null);
    }
}
