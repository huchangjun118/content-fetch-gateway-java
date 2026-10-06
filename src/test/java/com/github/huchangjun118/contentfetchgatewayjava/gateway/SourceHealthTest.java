package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Sources that keep failing or serving corrupt data are quarantined for a
 * period and rejoin scheduling only after a successful probe.
 */
class SourceHealthTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));

    private FakeContentSource source(String id) {
        return new FakeContentSource(id, 0, 1.0, 8,
                FakeContentSource.storeOf("x".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void quarantineAfterConsecutiveFailuresAndProbeRecovery() {
        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), clock);
        FakeContentSource s = source("flaky");

        health.recordFailure(s);
        health.recordFailure(s);
        assertTrue(health.isSchedulable(s), "below threshold: still schedulable");

        health.recordFailure(s);
        assertTrue(health.isQuarantined(s));
        assertFalse(health.isSchedulable(s), "quarantined: out of scheduling");

        // Quarantine expires but the probe still fails: stays out.
        clock.advance(Duration.ofMinutes(6));
        s.probeResult = false;
        assertFalse(health.isSchedulable(s));
        assertTrue(health.isQuarantined(s), "failed probe extends quarantine");

        // Probe recovers: source rejoins scheduling.
        clock.advance(Duration.ofMinutes(6));
        s.probeResult = true;
        assertTrue(health.isSchedulable(s));
        assertFalse(health.isQuarantined(s));
    }

    @Test
    void successResetsFailureCount() {
        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), clock);
        FakeContentSource s = source("flaky");

        health.recordFailure(s);
        health.recordFailure(s);
        health.recordSuccess(s);
        health.recordFailure(s);
        health.recordFailure(s);
        assertFalse(health.isQuarantined(s), "success reset the counter");
    }

    @Test
    void corruptionPenalisedHarderThanFailure() {
        SourceHealthRegistry health = new SourceHealthRegistry(3, Duration.ofMinutes(5), clock);
        FakeContentSource s = source("corrupt");

        health.recordCorruption(s); // penalty 2 at full trust
        assertTrue(health.isSchedulable(s));
        health.recordCorruption(s); // total 4 >= 3
        assertTrue(health.isQuarantined(s));
    }

    @Test
    void gatewaySkipsQuarantinedSources() {
        byte[] content = "content served only by the healthy source".getBytes(StandardCharsets.UTF_8);
        ContentId id = ContentId.of(content);
        Map<ContentId, byte[]> store = FakeContentSource.storeOf(content);

        FakeContentSource primary = new FakeContentSource("primary", 0, 1.0, 8, store);
        FakeContentSource secondary = new FakeContentSource("secondary", 1, 1.0, 8, store);

        SourceHealthRegistry health = new SourceHealthRegistry(1, Duration.ofMinutes(5), clock);
        GatewayProperties p = new GatewayProperties();
        p.setFetchTimeout(Duration.ofSeconds(5));
        GatewayService gateway = new GatewayService(List.of(primary, secondary), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), p);

        // One failure (threshold 1) quarantines the primary.
        primary.failAlways = true;
        assertArrayEquals(content, gateway.get(id).join());
        assertTrue(health.isQuarantined(primary));
        int manifestCallsBefore = primary.manifestCalls.get();

        // Next read must not even ask the quarantined primary.
        primary.failAlways = false;
        assertArrayEquals(content, gateway.get(id).join());
        // Cache may serve the second read; force a refetch to observe scheduling.
        gateway.shutdown();

        GatewayService gateway2 = new GatewayService(List.of(primary, secondary), health,
                new InMemoryContentCache(Duration.ofMinutes(10), Clock.systemUTC()), p);
        assertArrayEquals(content, gateway2.get(id).join());
        assertEquals(manifestCallsBefore, primary.manifestCalls.get(),
                "quarantined primary must be skipped by the scheduler");
        gateway2.shutdown();
    }
}
