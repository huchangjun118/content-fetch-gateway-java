package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.nio.file.Path;
import java.util.List;

/**
 * 测试辅助：在临时目录上构建网关。
 */
final class Gateways {

    private Gateways() {
    }

    static FetchGateway newGateway(Path tempDir, GatewayProperties props, ContentSource... sources) {
        props.setCacheDir(tempDir.resolve("cache").toString());
        props.setStagingDir(tempDir.resolve("staging").toString());
        return new FetchGateway(
                List.of(sources),
                new SourceHealthTracker(props.getFailureThreshold(), props.getOpenCooldown().toMillis()),
                new ContentCache(Path.of(props.getCacheDir()), props.getCacheTtl().toMillis()),
                new StagingStore(Path.of(props.getStagingDir())),
                props);
    }
}
