package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.time.Clock;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the gateway. Sources are contributed as beans; tests register fakes.
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfig {

    @Bean
    public SourceHealthRegistry sourceHealthRegistry(GatewayProperties properties) {
        return new SourceHealthRegistry(
                properties.getMaxConsecutiveFailures(),
                properties.getQuarantinePeriod(),
                Clock.systemUTC());
    }

    @Bean
    public ContentCache contentCache(GatewayProperties properties) {
        return new InMemoryContentCache(properties.getCacheTtl(), Clock.systemUTC());
    }

    @Bean
    public GatewayService gatewayService(List<ContentSource> sources,
                                         SourceHealthRegistry health,
                                         ContentCache cache,
                                         GatewayProperties properties) {
        return new GatewayService(sources, health, cache, properties);
    }
}
