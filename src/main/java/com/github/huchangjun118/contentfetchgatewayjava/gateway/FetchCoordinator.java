package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.io.ByteArrayOutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates fetching: single-flight merging of concurrent readers, source
 * selection by priority and health, per-chunk verification, resume from the
 * last verified position, and publication of only fully verified content.
 *
 * <p>Consistency: every chunk is verified against the content-addressed
 * manifest before it is cached or assembled, and the assembled content is
 * verified against the content address before it is returned or published.
 * Unverified fragments from different sources are therefore never spliced
 * into a result; only individually verified chunks may be reused.
 */
public class FetchCoordinator {

    private final List<ContentSource> sources;
    private final SourceHealthRegistry health;
    private final ContentCache cache;
    private final GatewayProperties properties;
    private final ExecutorService executor;
    private final ConcurrentMap<ContentId, CompletableFuture<byte[]>> inFlight = new ConcurrentHashMap<>();

    public FetchCoordinator(List<ContentSource> sources,
                            SourceHealthRegistry health,
                            ContentCache cache,
                            GatewayProperties properties) {
        this.sources = sources.stream()
                .sorted(Comparator.comparingInt(ContentSource::priority))
                .toList();
        this.health = health;
        this.cache = cache;
        this.properties = properties;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "fetch-coordinator");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Get the full content. Concurrent calls for the same id share one fetch.
     * The returned future completes only with a fully verified copy; cancelling
     * it detaches the caller but never leaves partial content visible.
     */
    public CompletableFuture<byte[]> get(ContentId id) {
        Optional<byte[]> cached = cache.getContent(id);
        if (cached.isPresent()) {
            return CompletableFuture.completedFuture(cached.get());
        }
        CompletableFuture<byte[]> leader = inFlight.computeIfAbsent(id, k -> {
            CompletableFuture<byte[]> f = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    f.complete(fetch(k));
                } catch (Throwable t) {
                    f.completeExceptionally(t);
                }
            });
            f.whenComplete((r, t) -> inFlight.remove(k, f));
            return f;
        });
        // copy(): a caller cancelling or timing out detaches without
        // disturbing other readers sharing the same in-flight fetch.
        return leader.copy().orTimeout(properties.getFetchTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private byte[] fetch(ContentId id) {
        Optional<byte[]> cached = cache.getContent(id);
        if (cached.isPresent()) {
            return cached.get();
        }
        SourceException lastError = null;
        for (ContentSource source : sources) {
            if (!health.isSchedulable(source)) {
                continue;
            }
            try {
                byte[] content = attemptWithTimeout(source, id);
                health.recordSuccess(source);
                return content;
            } catch (CorruptContentException e) {
                health.recordCorruption(source);
                lastError = e;
            } catch (SourceException e) {
                health.recordFailure(source);
                lastError = e;
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new SourceException("gateway", "no schedulable source for " + id);
    }

    /** Bound a single source attempt so a slow source cannot stall readers. */
    private byte[] attemptWithTimeout(ContentSource source, ContentId id) {
        Future<byte[]> future = executor.submit(() -> fetchFrom(source, id));
        try {
            return future.get(properties.getSourceTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new SourceException(source.id(), "source attempt timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new SourceException(source.id(), "interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SourceException se) {
                throw se;
            }
            throw new SourceException(source.id(), "unexpected source error", cause);
        }
    }

    /**
     * Fetch the whole content from one source, resuming from chunks already
     * verified in the cache. Verified chunks are neither re-downloaded nor
     * re-stored. Only a fully verified copy is published.
     */
    private byte[] fetchFrom(ContentSource source, ContentId id) {
        ContentManifest manifest = source.manifest(id);
        if (manifest == null || !manifest.id().equals(id)) {
            throw new CorruptContentException(source.id(), "manifest does not match " + id);
        }
        int chunkCount = manifest.chunkCount();
        byte[][] chunks = new byte[chunkCount][];
        for (int i = 0; i < chunkCount; i++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new SourceException(source.id(), "fetch cancelled");
            }
            Optional<byte[]> cachedChunk = cache.getVerifiedChunk(id, manifest, i);
            if (cachedChunk.isPresent()) {
                chunks[i] = cachedChunk.get();
                continue;
            }
            Chunk chunk = source.fetchChunk(id, i);
            if (chunk == null || chunk.index() != i || !manifest.verifyChunk(i, chunk.data())) {
                throw new CorruptContentException(source.id(), "chunk " + i + " failed verification");
            }
            cache.putChunk(id, i, chunk.data());
            chunks[i] = chunk.data();
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(manifest.size(), Integer.MAX_VALUE));
        for (byte[] chunk : chunks) {
            out.writeBytes(chunk);
        }
        byte[] content = out.toByteArray();
        if (!manifest.verifyContent(content)) {
            throw new CorruptContentException(source.id(), "assembled content failed verification");
        }
        cache.putContent(id, content);
        return content;
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
