package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Slow sources are bounded by timeouts, callers may cancel, and an unfinished
 * fetch never leaves half a content visible to later readers.
 */
class CancellationTimeoutTest {

    private static final int CHUNK = 8;

    private GatewayProperties properties(Duration fetchTimeout, Duration sourceTimeout) {
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(fetchTimeout);
        p.setSourceTimeout(sourceTimeout);
        p.setMaxConsecutiveFailures(10);
        p.setQuarantinePeriod(Duration.ofMinutes(5));
        p.setCacheTtl(Duration.ofMinutes(10));
        return p;
    }

    @Test
    void slowSourceTimesOutAndFallsOver() {
        byte[] content = "content behind one slow and one fast source".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource slow = new FakeContentSource("slow", 0, 1.0, CHUNK, store);
        slow.latencyMillis = 1500;
        FakeContentSource fast = new FakeContentSource("fast", 1, 1.0, CHUNK, store);

        SourceHealthRegistry health = new SourceHealthRegistry(10, Duration.ofMinutes(5), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(slow, fast), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()),
                properties(Duration.ofSeconds(10), Duration.ofMillis(300)));

        byte[] result = gateway.get(id).join();

        assertArrayEquals(content, result);
        assertTrue(fast.chunkCalls.get() > 0, "fell over to the fast source");
        gateway.shutdown();
    }

    @Test
    void fetchTimeoutFailsWithoutPublishingPartialContent() {
        byte[] content = "content that only a very slow source can serve".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource slow = new FakeContentSource("slow", 0, 1.0, CHUNK, store);
        slow.latencyMillis = 2000;

        SourceHealthRegistry health = new SourceHealthRegistry(10, Duration.ofMinutes(5), Clock.systemUTC());
        InMemoryContentCache cache = new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(slow), health, cache,
                properties(Duration.ofMillis(400), Duration.ofSeconds(5)));

        CompletableFuture<byte[]> future = gateway.get(id);
        ExecutionException error = assertThrows(ExecutionException.class, future::get);
        assertTrue(error.getCause() instanceof TimeoutException,
                "expected fetch timeout, got " + error.getCause());
        assertTrue(cache.getContent(id).isEmpty(),
                "timed-out fetch must not publish partial content");
        gateway.shutdown();
    }

    @Test
    void cancelledReadLeavesNoPartialContentAndLaterReadsWork() throws Exception {
        byte[] content = "content first read is cancelled midway".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource source = new FakeContentSource("s", 0, 1.0, CHUNK, store);
        source.latencyMillis = 400;

        SourceHealthRegistry health = new SourceHealthRegistry(10, Duration.ofMinutes(5), Clock.systemUTC());
        InMemoryContentCache cache = new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(source), health, cache,
                properties(Duration.ofSeconds(10), Duration.ofSeconds(5)));

        CompletableFuture<byte[]> cancelled = gateway.get(id);
        cancelled.cancel(true);
        assertTrue(cancelled.isCancelled());

        // A later reader gets a complete, verified copy.
        source.latencyMillis = 0;
        byte[] result = gateway.get(id).get(5, TimeUnit.SECONDS);
        assertArrayEquals(content, result);
        assertArrayEquals(content, cache.getContent(id).orElseThrow(),
                "only fully verified content may be cached");
        gateway.shutdown();
    }
}
