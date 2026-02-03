package net.snowflake.ingest.streaming;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import net.snowflake.ingest.streaming.internal.ColumnProperties;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestChannel} which stores state in memory. This
 * implementation is used for testing and is able to keep state between calls: so inserting rows
 * through {@link SnowflakeStreamingIngestChannel#insertRow} will update the {@link
 * SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}.
 *
 * <p>Note that this implementation is not thread safe and some functionality may be missing.
 */
public class FakeSnowflakeStreamingIngestChannel implements SnowflakeStreamingIngestChannel {
    private final String name;
    private final String fullyQualifiedName;
    private final String dbName;
    private final String schemaName;
    private final String tableName;
    private final String fullyQualifiedTableName;
    private boolean closed;
    private String offsetToken;

    private final List<Map<String, Object>> rows = new LinkedList<>();

    public FakeSnowflakeStreamingIngestChannel(
            String name, String dbName, String schemaName, String tableName) {
        this.name = name;
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fullyQualifiedName = String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name);
        this.fullyQualifiedTableName = String.format("%s.%s.%s", dbName, schemaName, tableName);
    }

    @Override
    public String getFullyQualifiedName() {
        return fullyQualifiedName;
    }

    @Override
    public String getName() {
        return name;
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
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getFullyQualifiedTableName() {
        return fullyQualifiedTableName;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public CompletableFuture<Void> close() {
        return this.close(false);
    }

    @Override
    public CompletableFuture<Void> close(boolean b) {
        closed = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public InsertValidationResponse insertRow(Map<String, Object> row, String offsetToken) {
        return this.insertRows(Lists.newArrayList(row), offsetToken);
    }

    @Override
    public InsertValidationResponse insertRows(
            Iterable<Map<String, Object>> iterable,
            String startOffsetToken,
            String endOffsetToken) {
        final List<Map<String, Object>> rowsCopy = new LinkedList<>();
        rows.forEach(r -> rowsCopy.add(new LinkedHashMap<>(r)));
        this.rows.addAll(rowsCopy);
        this.offsetToken = endOffsetToken;
        return new InsertValidationResponse();
    }

    @Override
    public InsertValidationResponse insertRows(
            Iterable<Map<String, Object>> rows, String offsetToken) {
        return this.insertRows(rows, null, offsetToken);
    }

    @Override
    public String getLatestCommittedOffsetToken() {
        return offsetToken;
    }

    /**
     * Flush the channel, returning a CompletableFuture that completes immediately. This simulates
     * the async flush operation.
     *
     * @param ignored whether this is a closing flush
     * @return a completed CompletableFuture
     */
    public CompletableFuture<Void> flush(boolean ignored) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Map<String, ColumnProperties> getTableSchema() {
        throw new UnsupportedOperationException(
                "Method is unsupported in fake communication channel");
    }
}
