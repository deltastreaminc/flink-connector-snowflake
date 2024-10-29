package net.snowflake.ingest.streaming;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestClient} which stores state in memory. This
 * implementation is used for testing and works together with {@link
 * FakeSnowflakeStreamingIngestChannel} for simulating ingest-sdk.
 *
 * <p>Note that this implementation is not thread safe and some functionality may be missing.
 */
public class FakeSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {

    private final String name;
    private boolean closed;
    private final ConcurrentHashMap<String, FakeSnowflakeStreamingIngestChannel> channelCache =
            new ConcurrentHashMap<>();

    public FakeSnowflakeStreamingIngestClient(String name) {
        this.name = name;
    }

    @Override
    public SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest request) {
        String fqdn =
                String.format(
                        "%s.%s", request.getFullyQualifiedTableName(), request.getChannelName());
        return channelCache.computeIfAbsent(
                fqdn,
                (key) ->
                        new FakeSnowflakeStreamingIngestChannel(
                                name,
                                request.getDBName(),
                                request.getSchemaName(),
                                request.getTableName()));
    }

    @Override
    public void dropChannel(DropChannelRequest dropChannelRequest) {
        channelCache.remove(
                String.format(
                        "%s.%s",
                        dropChannelRequest.getFullyQualifiedTableName(),
                        dropChannelRequest.getChannelName()));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setRefreshToken(String refreshToken) {}

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Map<String, String> getLatestCommittedOffsetTokens(
            List<SnowflakeStreamingIngestChannel> channels) {
        Map<String, String> offsetTokens = new HashMap<>();
        channels.forEach(
                c -> {
                    String fqn = c.getFullyQualifiedName();
                    String token = channelCache.get(fqn).getLatestCommittedOffsetToken();
                    offsetTokens.put(fqn, token);
                });
        return offsetTokens;
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }
}
