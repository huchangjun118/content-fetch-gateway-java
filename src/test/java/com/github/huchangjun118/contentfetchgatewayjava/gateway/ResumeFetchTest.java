package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * An interrupted fetch resumes from the last verified position: verified
 * chunks are neither re-downloaded nor stored again.
 */
class ResumeFetchTest {

    private static final int CHUNK = 8;

    /** Counts putChunk calls per chunk index. */
    private static final class CountingCache implements ContentCache {
        private final ContentCache delegate;
        private final Map<Integer, AtomicInteger> puts = new ConcurrentHashMap<>();

        CountingCache(ContentCache delegate) {
            this.delegate = delegate;
        }

        int putsOf(int index) {
            AtomicInteger n = puts.get(index);
            return n == null ? 0 : n.get();
        }

        @Override
        public Optional<byte[]> getVerifiedChunk(ContentId id, ContentManifest manifest, int chunkIndex) {
            return delegate.getVerifiedChunk(id, manifest, chunkIndex);
        }

        @Override
        public void putChunk(ContentId id, int chunkIndex, byte[] data) {
            puts.computeIfAbsent(chunkIndex, k -> new AtomicInteger()).incrementAndGet();
            delegate.putChunk(id, chunkIndex, data);
        }

        @Override
        public Optional<byte[]> getContent(ContentId id) {
            return delegate.getContent(id);
        }

        @Override
        public void putContent(ContentId id, byte[] content) {
            delegate.putContent(id, content);
        }

        @Override
        public void evict(ContentId id) {
            delegate.evict(id);
        }
    }

    private GatewayProperties properties() {
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(Duration.ofSeconds(5));
        p.setSourceTimeout(Duration.ofSeconds(2));
        p.setMaxConsecutiveFailures(10);
        p.setQuarantinePeriod(Duration.ofMinutes(5));
        p.setCacheTtl(Duration.ofMinutes(10));
        return p;
    }

    @Test
    void interruptedFetchResumesFromVerifiedPosition() {
        // 5 chunks of 8 bytes.
        byte[] content = "0123456789abcdef0123456789abcdef01234567".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource primary = new FakeContentSource("primary", 0, 1.0, CHUNK, store);
        // Serves manifest + chunks 0 and 1, then dies mid-content.
        primary.successBudget.set(3);
        FakeContentSource secondary = new FakeContentSource("secondary", 1, 1.0, CHUNK, store);

        SourceHealthRegistry health = new SourceHealthRegistry(10, Duration.ofMinutes(5), Clock.systemUTC());
        CountingCache cache = new CountingCache(new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()));
        GatewayService gateway = new GatewayService(List.of(primary, secondary), health, cache, properties());

        byte[] result = gateway.get(id).join();

        assertArrayEquals(content, result);
        assertEquals(1, primary.chunkCallCounts.get(0).get());
        assertEquals(1, primary.chunkCallCounts.get(1).get());
        // Secondary resumed from chunk 2: verified chunks not re-downloaded.
        assertFalse(secondary.chunkCallCounts.containsKey(0), "chunk 0 must come from cache");
        assertFalse(secondary.chunkCallCounts.containsKey(1), "chunk 1 must come from cache");
        assertEquals(1, secondary.chunkCallCounts.get(2).get());
        // Verified chunks stored exactly once: no duplicate writes.
        assertEquals(1, cache.putsOf(0));
        assertEquals(1, cache.putsOf(1));
        gateway.shutdown();
    }

    @Test
    void failedReadResumesAcrossSeparateRequests() {
        byte[] content = "0123456789abcdef0123456789abcdef01234567".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource primary = new FakeContentSource("primary", 0, 1.0, CHUNK, store);
        primary.successBudget.set(3); // manifest + chunks 0,1, then dies
        FakeContentSource secondary = new FakeContentSource("secondary", 1, 1.0, CHUNK, store);
        secondary.failAlways = true;

        SourceHealthRegistry health = new SourceHealthRegistry(10, Duration.ofMinutes(5), Clock.systemUTC());
        CountingCache cache = new CountingCache(new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()));
        GatewayService gateway = new GatewayService(List.of(primary, secondary), health, cache, properties());

        // First read fails: no source can complete. Verified chunks survive.
        assertThrows(Exception.class, () -> gateway.get(id).join());
        assertTrue(cache.getContent(id).isEmpty(), "partial content must never be published");

        // Sources recover; second read resumes at chunk 2.
        primary.successBudget.set(10);
        secondary.failAlways = false;
        byte[] result = gateway.get(id).join();

        assertArrayEquals(content, result);
        assertEquals(1, primary.chunkCallCounts.get(0).get(), "chunk 0 downloaded only once");
        assertEquals(1, primary.chunkCallCounts.get(1).get(), "chunk 1 downloaded only once");
        assertEquals(1, cache.putsOf(0));
        assertEquals(1, cache.putsOf(1));
        gateway.shutdown();
    }
}
