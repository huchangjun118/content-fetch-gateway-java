package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/**
 * 网关 Bean 装配。内容来源以 {@link ContentSource} Bean 的形式注册，
 * 本地测试用测试替身，生产环境对接真实来源客户端，均不依赖外部凭据。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    @Bean
    public SourceHealthTracker sourceHealthTracker(GatewayProperties properties) {
        return new SourceHealthTracker(
                properties.getFailureThreshold(),
                properties.getOpenCooldown().toMillis());
    }

    @Bean
    public ContentCache contentCache(GatewayProperties properties) {
        return new ContentCache(
                Path.of(properties.getCacheDir()),
                properties.getCacheTtl().toMillis());
    }

    @Bean
    public StagingStore stagingStore(GatewayProperties properties) {
        return new StagingStore(Path.of(properties.getStagingDir()));
    }

    @Bean
    public FetchGateway fetchGateway(List<ContentSource> sources,
                                     SourceHealthTracker healthTracker,
                                     ContentCache contentCache,
                                     StagingStore stagingStore,
                                     GatewayProperties properties) {
        return new FetchGateway(sources, healthTracker, contentCache, stagingStore, properties);
    }
}
