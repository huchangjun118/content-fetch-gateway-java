package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Concurrent readers of the same content share one fetch; a slow source must
 * not stall readers whose content is already available.
 */
class ConcurrencyTest {

    private static final int CHUNK = 16;

    private GatewayProperties properties() {
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(Duration.ofSeconds(10));
        p.setSourceTimeout(Duration.ofSeconds(5));
        p.setMaxConsecutiveFailures(3);
        p.setQuarantinePeriod(Duration.ofMinutes(5));
        p.setCacheTtl(Duration.ofMinutes(10));
        return p;
    }

    @Test
    void concurrentReadsAreMergedIntoOneFetch() throws Exception {
        byte[] content = "shared content fetched once for many concurrent readers".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource source = new FakeContentSource("only", 0, 1.0, CHUNK, store);
        source.latencyMillis = 30; // make overlap likely

        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(source), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), properties());

        int readers = 16;
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            futures.add(gateway.get(id));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);

        for (CompletableFuture<byte[]> f : futures) {
            assertArrayEquals(content, f.join());
        }
        int chunkCount = (content.length + CHUNK - 1) / CHUNK;
        assertEquals(chunkCount, source.chunkCalls.get(),
                "single-flight: chunks fetched exactly once for all readers");
        assertEquals(1, source.manifestCalls.get(),
                "single-flight: manifest fetched exactly once");
        gateway.shutdown();
    }

    @Test
    void slowFetchDoesNotStallReadersOfCachedContent() throws Exception {
        byte[] cached = "already available content".getBytes(StandardCharsets.UTF_8);
        byte[] slow = "brand new content behind a slow source".getBytes(StandardCharsets.UTF_8);
        ContentId cachedId = ContentId.of(cached);
        ContentId slowId = ContentId.of(slow);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(cached, slow);

        FakeContentSource source = new FakeContentSource("slow", 0, 1.0, CHUNK, store);
        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(source), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), properties());

        // Warm the cache for `cached`, then make the source slow.
        assertArrayEquals(cached, gateway.get(cachedId).join());
        source.latencyMillis = 800;

        CompletableFuture<byte[]> slowRead = gateway.get(slowId);
        long start = System.nanoTime();
        byte[] hit = gateway.get(cachedId).get(2, TimeUnit.SECONDS);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertArrayEquals(cached, hit);
        assertTrue(elapsedMillis < 500,
                "cache hit must not wait for the slow in-flight fetch, took " + elapsedMillis + "ms");
        assertArrayEquals(slow, slowRead.get(10, TimeUnit.SECONDS));
        gateway.shutdown();
    }
}
