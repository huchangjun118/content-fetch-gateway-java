package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-source health. Sources that keep failing or serving corrupt data
 * are quarantined for a configurable period and only rejoin scheduling after a
 * successful probe. Corruption is penalised harder than transport failure,
 * scaled by the source's declared trust.
 */
public class SourceHealthRegistry {

    private static final class State {
        private int consecutiveFailures;
        private Instant quarantinedUntil = Instant.MIN;
    }

    private final int maxConsecutiveFailures;
    private final Duration quarantinePeriod;
    private final Clock clock;
    private final Map<String, State> states = new ConcurrentHashMap<>();

    public SourceHealthRegistry(int maxConsecutiveFailures, Duration quarantinePeriod, Clock clock) {
        this.maxConsecutiveFailures = maxConsecutiveFailures;
        this.quarantinePeriod = quarantinePeriod;
        this.clock = clock;
    }

    private State state(ContentSource source) {
        return states.computeIfAbsent(source.id(), k -> new State());
    }

    /**
     * Whether the source may be scheduled right now. A source whose quarantine
     * just expired is probed once: it rejoins only if the probe succeeds.
     */
    public boolean isSchedulable(ContentSource source) {
        State state = state(source);
        synchronized (state) {
            if (clock.instant().isBefore(state.quarantinedUntil)) {
                return false;
            }
            boolean wasQuarantined = !Instant.MIN.equals(state.quarantinedUntil);
            if (wasQuarantined) {
                if (source.probe()) {
                    state.consecutiveFailures = 0;
                    state.quarantinedUntil = Instant.MIN;
                    return true;
                }
                // Probe failed: stay out of scheduling for another period.
                state.quarantinedUntil = clock.instant().plus(quarantinePeriod);
                return false;
            }
            return true;
        }
    }

    public void recordSuccess(ContentSource source) {
        State state = state(source);
        synchronized (state) {
            state.consecutiveFailures = 0;
            state.quarantinedUntil = Instant.MIN;
        }
    }

    /** Record a transport-level failure. */
    public void recordFailure(ContentSource source) {
        penalize(source, 1);
    }

    /** Record that the source served corrupt data (weighted by trust). */
    public void recordCorruption(ContentSource source) {
        int penalty = Math.max(1, (int) Math.ceil(2 * source.trust()));
        penalize(source, penalty);
    }

    private void penalize(ContentSource source, int weight) {
        State state = state(source);
        synchronized (state) {
            state.consecutiveFailures += weight;
            if (state.consecutiveFailures >= maxConsecutiveFailures) {
                state.quarantinedUntil = clock.instant().plus(quarantinePeriod);
            }
        }
    }

    /** Visible for tests: is the source currently quarantined. */
    public boolean isQuarantined(ContentSource source) {
        State state = state(source);
        synchronized (state) {
            return clock.instant().isBefore(state.quarantinedUntil);
        }
    }
}
