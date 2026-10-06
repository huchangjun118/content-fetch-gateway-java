package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TTL-based in-memory cache. Every read re-verifies integrity so a corrupted
 * entry heals itself by being evicted instead of served. Entries are immutable
 * byte arrays, so evicting or replacing them never disturbs readers that
 * already hold a reference.
 */
public class InMemoryContentCache implements ContentCache {

    private record Timed(byte[] data, Instant storedAt) {
    }

    private static final class Entry {
        private volatile Timed content;
        private final Map<Integer, Timed> chunks = new ConcurrentHashMap<>();
    }

    private final Duration ttl;
    private final Clock clock;
    private final Map<ContentId, Entry> entries = new ConcurrentHashMap<>();

    public InMemoryContentCache(Duration ttl, Clock clock) {
        this.ttl = ttl;
        this.clock = clock;
    }

    private boolean fresh(Timed timed) {
        return timed != null
                && timed.storedAt().plus(ttl).isAfter(clock.instant());
    }

    @Override
    public Optional<byte[]> getVerifiedChunk(ContentId id, ContentManifest manifest, int chunkIndex) {
        Entry entry = entries.get(id);
        if (entry == null) {
            return Optional.empty();
        }
        Timed timed = entry.chunks.get(chunkIndex);
        if (!fresh(timed)) {
            if (timed != null) {
                entry.chunks.remove(chunkIndex, timed);
            }
            return Optional.empty();
        }
        // Self-healing: never serve a chunk that no longer verifies.
        if (!manifest.verifyChunk(chunkIndex, timed.data())) {
            entry.chunks.remove(chunkIndex, timed);
            return Optional.empty();
        }
        return Optional.of(timed.data());
    }

    @Override
    public void putChunk(ContentId id, int chunkIndex, byte[] data) {
        entries.computeIfAbsent(id, k -> new Entry())
                .chunks.put(chunkIndex, new Timed(data, clock.instant()));
    }

    @Override
    public Optional<byte[]> getContent(ContentId id) {
        Entry entry = entries.get(id);
        if (entry == null) {
            return Optional.empty();
        }
        Timed timed = entry.content;
        if (!fresh(timed)) {
            return Optional.empty();
        }
        // Self-healing: a cached content that fails verification is dropped.
        if (!ContentId.of(timed.data()).equals(id)) {
            evict(id);
            return Optional.empty();
        }
        return Optional.of(timed.data());
    }

    @Override
    public void putContent(ContentId id, byte[] content) {
        entries.computeIfAbsent(id, k -> new Entry())
                .content = new Timed(content, clock.instant());
    }

    @Override
    public void evict(ContentId id) {
        entries.remove(id);
    }
}
