package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景：持续失败 / 反复返回损坏内容的来源被熔断退出调度，
 * 冷却探测恢复正常后重新参与调度。
 */
class SourceHealthTest {

    @TempDir
    Path tempDir;

    private GatewayProperties props() {
        GatewayProperties props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setOpenCooldown(Duration.ofMillis(200));
        return props;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void failingSourceIsTakenOutOfScheduling() throws Exception {
        GatewayProperties props = props();
        // 关闭缓存，确保每次获取都回源
        props.setCacheTtl(Duration.ZERO);
        FakeContentSource down = new FakeContentSource("down", 1, TrustLevel.STANDARD, 8);
        FakeContentSource backup = new FakeContentSource("backup", 2, TrustLevel.STANDARD, 8);
        byte[] content = bytes("backup serves while primary is down");
        down.publish(content);
        String cid = backup.publish(content);
        down.injectErrors(100);

        SourceHealthTracker tracker = new SourceHealthTracker(
                props.getFailureThreshold(), props.getOpenCooldown().toMillis());
        props.setCacheDir(tempDir.resolve("cache").toString());
        props.setStagingDir(tempDir.resolve("staging").toString());
        try (FetchGateway gateway = new FetchGateway(
                java.util.List.of(down, backup), tracker,
                new ContentCache(Path.of(props.getCacheDir()), props.getCacheTtl().toMillis()),
                new StagingStore(Path.of(props.getStagingDir())), props)) {

            // 每次获取都会让 down 记录失败，达到阈值后熔断
            for (int i = 0; i < 3; i++) {
                FetchResult r = gateway.fetch(cid);
                assertArrayEquals(content, r.content());
                assertEquals("backup", r.sourceId());
            }
            assertEquals(SourceHealthTracker.State.OPEN, tracker.state("down"));

            int manifestCallsBefore = down.manifestCalls();
            // 缓存未命中（换一个新 cid）时也不应再调度 down
            byte[] content2 = bytes("another piece of content");
            down.publish(content2);
            String cid2 = backup.publish(content2);
            FetchResult r2 = gateway.fetch(cid2);
            assertEquals("backup", r2.sourceId());
            assertEquals(manifestCallsBefore, down.manifestCalls(),
                    "熔断期间 down 不应再被调度");
        }
    }

    @Test
    void sourceRejoinsAfterCooldownProbeSucceeds() throws Exception {
        GatewayProperties props = props();
        FakeContentSource flaky = new FakeContentSource("flaky", 1, TrustLevel.STANDARD, 8);
        byte[] content = bytes("flaky source recovers after cooldown");
        String cid = flaky.publish(content);

        SourceHealthTracker tracker = new SourceHealthTracker(
                props.getFailureThreshold(), props.getOpenCooldown().toMillis());
        props.setCacheDir(tempDir.resolve("cache").toString());
        props.setStagingDir(tempDir.resolve("staging").toString());
        try (FetchGateway gateway = new FetchGateway(
                java.util.List.of(flaky), tracker,
                new ContentCache(Path.of(props.getCacheDir()), props.getCacheTtl().toMillis()),
                new StagingStore(Path.of(props.getStagingDir())), props)) {

            flaky.injectErrors(100);
            assertThrows(FetchException.class, () -> gateway.fetch(cid));
            assertThrows(FetchException.class, () -> gateway.fetch(cid));
            assertEquals(SourceHealthTracker.State.OPEN, tracker.state("flaky"));

            // 冷却期内即使来源恢复也不参与调度
            flaky.injectErrors(0);
            assertThrows(FetchException.class, () -> gateway.fetch(cid));

            // 冷却结束后探测流量进入，成功后恢复
            Thread.sleep(250);
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals(SourceHealthTracker.State.CLOSED, tracker.state("flaky"));
        }
    }

    @Test
    void corruptSourceIsCircuitBrokenFaster() throws Exception {
        GatewayProperties props = props();
        FakeContentSource corrupt = new FakeContentSource("corrupt", 1, TrustLevel.STANDARD, 8);
        FakeContentSource healthy = new FakeContentSource("healthy", 2, TrustLevel.STANDARD, 8);
        byte[] content = bytes("corrupt sources are punished harder");
        corrupt.publish(content);
        String cid = healthy.publish(content);
        corrupt.setCorruptChunks(true);

        SourceHealthTracker tracker = new SourceHealthTracker(
                props.getFailureThreshold(), props.getOpenCooldown().toMillis());
        props.setCacheDir(tempDir.resolve("cache").toString());
        props.setStagingDir(tempDir.resolve("staging").toString());
        try (FetchGateway gateway = new FetchGateway(
                java.util.List.of(corrupt, healthy), tracker,
                new ContentCache(Path.of(props.getCacheDir()), props.getCacheTtl().toMillis()),
                new StagingStore(Path.of(props.getStagingDir())), props)) {

            // 损坏按两次失败计，一次获取即达到阈值 2
            FetchResult r = gateway.fetch(cid);
            assertEquals("healthy", r.sourceId());
            assertEquals(SourceHealthTracker.State.OPEN, tracker.state("corrupt"));
        }
    }
}
