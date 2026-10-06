package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunables for the gateway, prefix {@code gateway}.
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** Max time for a whole content fetch before it is failed. */
    private Duration fetchTimeout = Duration.ofSeconds(30);

    /** Max time a single source attempt may take before falling over. */
    private Duration sourceTimeout = Duration.ofSeconds(5);

    /** Consecutive failures before a source is quarantined. */
    private int maxConsecutiveFailures = 3;

    /** How long a quarantined source stays out of scheduling. */
    private Duration quarantinePeriod = Duration.ofSeconds(30);

    /** Cache entry time-to-live. */
    private Duration cacheTtl = Duration.ofMinutes(5);

    public Duration getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(Duration fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public Duration getSourceTimeout() {
        return sourceTimeout;
    }

    public void setSourceTimeout(Duration sourceTimeout) {
        this.sourceTimeout = sourceTimeout;
    }

    public int getMaxConsecutiveFailures() {
        return maxConsecutiveFailures;
    }

    public void setMaxConsecutiveFailures(int maxConsecutiveFailures) {
        this.maxConsecutiveFailures = maxConsecutiveFailures;
    }

    public Duration getQuarantinePeriod() {
        return quarantinePeriod;
    }

    public void setQuarantinePeriod(Duration quarantinePeriod) {
        this.quarantinePeriod = quarantinePeriod;
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(Duration cacheTtl) {
        this.cacheTtl = cacheTtl;
    }
}
