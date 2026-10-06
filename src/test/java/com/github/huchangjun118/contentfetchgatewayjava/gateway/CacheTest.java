package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Cache hits stay consistent with the source (TTL expiry forces refetch) and
 * corrupted cache entries heal themselves instead of being served.
 */
class CacheTest {

    private static final int CHUNK = 8;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));

    private GatewayProperties properties() {
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(Duration.ofSeconds(5));
        p.setSourceTimeout(Duration.ofSeconds(2));
        p.setCacheTtl(Duration.ofMinutes(5));
        return p;
    }

    private record Fixture(GatewayService gateway, InMemoryContentCache cache, FakeContentSource source) {
    }

    private Fixture fixture(byte[] content) {
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);
        FakeContentSource source = new FakeContentSource("s", 0, 1.0, CHUNK, store);
        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), clock);
        InMemoryContentCache cache = new InMemoryContentCache(Duration.ofMinutes(5), clock);
        GatewayService gateway = new GatewayService(List.of(source), health, cache, properties());
        return new Fixture(gateway, cache, source);
    }

    @Test
    void cacheHitAvoidsSourceAndExpiresByTtl() {
        byte[] content = "cacheable content with several chunks".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Fixture f = fixture(content);

        assertArrayEquals(content, f.gateway().get(id).join());
        int chunksAfterFirst = f.source().chunkCalls.get();

        // Fresh cache hit: no source traffic.
        assertArrayEquals(content, f.gateway().get(id).join());
        assertEquals(chunksAfterFirst, f.source().chunkCalls.get());

        // After TTL the entry is stale: content is refetched from the source,
        // so a hit can never diverge from what the source currently serves.
        clock.advance(Duration.ofMinutes(6));
        assertArrayEquals(content, f.gateway().get(id).join());
        assertEquals(chunksAfterFirst * 2, f.source().chunkCalls.get(),
                "expired entry must be refetched");
        f.gateway().shutdown();
    }

    @Test
    void corruptedCachedContentSelfHeals() {
        byte[] content = "content whose cached copy gets corrupted".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Fixture f = fixture(content);

        assertArrayEquals(content, f.gateway().get(id).join());

        // Simulate bit rot in the cached copy.
        byte[] tampered = content.clone();
        tampered[0] ^= 0x7F;
        f.cache().putContent(id, tampered);

        // The corrupted entry is detected and evicted; the read self-heals.
        assertArrayEquals(content, f.gateway().get(id).join());
        assertArrayEquals(content, f.cache().getContent(id).orElseThrow());
        f.gateway().shutdown();
    }

    @Test
    void corruptedCachedChunkSelfHeals() {
        byte[] content = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Fixture f = fixture(content);

        assertArrayEquals(content, f.gateway().get(id).join());
        int chunkCallsAfterFirst = f.source().chunkCalls.get();

        // Corrupt one cached chunk; full-content entry is dropped to force
        // the chunked path.
        f.cache().evict(id);
        ContentManifest manifest = f.source().manifest(id);
        f.cache().putChunk(id, 0, new byte[CHUNK]); // garbage chunk
        f.cache().putChunk(id, 1, "abcdefgh".getBytes(StandardCharsets.UTF_8)); // wrong hash

        assertArrayEquals(content, f.gateway().get(id).join());
        // Only the missing/invalid chunks were refetched.
        int refetched = f.source().chunkCalls.get() - chunkCallsAfterFirst;
        assertEquals(manifest.chunkCount(), refetched,
                "chunks re-verified on read; invalid ones refetched exactly once");
        f.gateway().shutdown();
    }
}
