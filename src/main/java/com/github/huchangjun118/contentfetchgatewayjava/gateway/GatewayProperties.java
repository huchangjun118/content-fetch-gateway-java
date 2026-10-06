package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 网关配置项。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /** 缓存目录（已校验内容落盘位置）。 */
    private String cacheDir = "./data/cache";

    /** 暂存目录（下载中、已校验分块的落盘位置）。 */
    private String stagingDir = "./data/staging";

    /** 缓存有效期，过期后访问将回源重新获取。 */
    private Duration cacheTtl = Duration.ofMinutes(10);

    /** 单次获取的整体超时。 */
    private Duration fetchTimeout = Duration.ofSeconds(30);

    /** 来源熔断：连续失败达到该次数后进入 OPEN。 */
    private int failureThreshold = 3;

    /** 来源熔断：OPEN 状态持续时间，到期后进入 HALF_OPEN 探测。 */
    private Duration openCooldown = Duration.ofSeconds(30);

    public String getCacheDir() {
        return cacheDir;
    }

    public void setCacheDir(String cacheDir) {
        this.cacheDir = cacheDir;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(Duration cacheTtl) {
        this.cacheTtl = cacheTtl;
    }

    public Duration getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(Duration fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public Duration getOpenCooldown() {
        return openCooldown;
    }

    public void setOpenCooldown(Duration openCooldown) {
        this.openCooldown = openCooldown;
    }
}
