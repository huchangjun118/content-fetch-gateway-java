package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A corrupt or failing source must not break the read: the gateway falls over
 * to the next source and returns the first fully verified copy.
 */
class MultiSourceFetchTest {

    private static final int CHUNK = 8;

    private GatewayProperties properties() {
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(Duration.ofSeconds(5));
        p.setSourceTimeout(Duration.ofSeconds(2));
        p.setMaxConsecutiveFailures(3);
        p.setQuarantinePeriod(Duration.ofMinutes(5));
        p.setCacheTtl(Duration.ofMinutes(10));
        return p;
    }

    @Test
    void corruptPrimaryFallsBackToSecondary() {
        byte[] content = "content-addressed payload across several chunks".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource primary = new FakeContentSource("primary", 0, 1.0, CHUNK, store);
        primary.corruptChunks = true;
        FakeContentSource secondary = new FakeContentSource("secondary", 1, 1.0, CHUNK, store);

        // Threshold 2: a single corrupt chunk (penalty 2 with full trust)
        // already quarantines the source.
        SourceHealthRegistry health = new SourceHealthRegistry(2, Duration.ofMinutes(5), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(primary, secondary), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), properties());

        byte[] result = gateway.get(id).join();

        assertArrayEquals(content, result);
        assertEquals(id, ContentId.of(result), "returned bytes must match the content address");
        assertTrue(secondary.chunkCalls.get() > 0, "secondary must have served chunks");
        assertTrue(health.isQuarantined(primary) || !health.isSchedulable(primary),
                "repeatedly corrupt primary must be penalised");
        gateway.shutdown();
    }

    @Test
    void flakyPrimaryRecoversWithinSameRead() {
        byte[] content = "another payload, primary fails once then secondary serves".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource primary = new FakeContentSource("primary", 0, 1.0, CHUNK, store);
        primary.failAlways = true;
        FakeContentSource secondary = new FakeContentSource("secondary", 1, 1.0, CHUNK, store);

        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), Clock.systemUTC());
        GatewayService gateway = new GatewayService(List.of(primary, secondary), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), properties());

        byte[] result = gateway.get(id).join();

        assertArrayEquals(content, result);
        assertFalse(health.isQuarantined(secondary));
        gateway.shutdown();
    }
}
