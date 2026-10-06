package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Public API of the gateway: stable, consistent reads over many sources.
 */
public class GatewayService {

    private final FetchCoordinator coordinator;

    public GatewayService(List<ContentSource> sources,
                          SourceHealthRegistry health,
                          ContentCache cache,
                          GatewayProperties properties) {
        this.coordinator = new FetchCoordinator(sources, health, cache, properties);
    }

    /**
     * Fetch the full, verified content for an address. The future fails with
     * {@link SourceException} if no source can serve a valid copy in time, or
     * with {@link java.util.concurrent.TimeoutException} when the configured
     * fetch timeout elapses. Cancelling the future detaches the caller; no
     * partial content is ever published as a result.
     */
    public CompletableFuture<byte[]> get(ContentId id) {
        return coordinator.get(id);
    }

    public void shutdown() {
        coordinator.shutdown();
    }
}
