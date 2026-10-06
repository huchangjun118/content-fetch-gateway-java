package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景：多来源调度、损坏内容回退、优先级与可信度排序。
 */
class MultiSourceFetchTest {

    @TempDir
    Path tempDir;

    private FetchGateway newGateway(GatewayProperties props, ContentSource... sources) {
        props.setCacheDir(tempDir.resolve("cache").toString());
        props.setStagingDir(tempDir.resolve("staging").toString());
        return new FetchGateway(
                List.of(sources),
                new SourceHealthTracker(props.getFailureThreshold(), props.getOpenCooldown().toMillis()),
                new ContentCache(Path.of(props.getCacheDir()), props.getCacheTtl().toMillis()),
                new StagingStore(Path.of(props.getStagingDir())),
                props);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void returnsVerifiedContentFromHighestPrioritySource() throws Exception {
        FakeContentSource low = new FakeContentSource("low", 2, TrustLevel.STANDARD, 8);
        FakeContentSource high = new FakeContentSource("high", 1, TrustLevel.STANDARD, 8);
        byte[] content = bytes("hello content addressed world");
        low.publish(content);
        String cid = high.publish(content);

        try (FetchGateway gateway = newGateway(new GatewayProperties(), low, high)) {
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals("high", result.sourceId());
            assertFalse(result.fromCache());
            assertEquals(0, low.totalChunkFetches(), "低优先级来源不应被调度");
        }
    }

    @Test
    void fallsBackToNextSourceWhenPreferredReturnsCorruptChunks() throws Exception {
        FakeContentSource corrupt = new FakeContentSource("corrupt", 1, TrustLevel.STANDARD, 8);
        FakeContentSource healthy = new FakeContentSource("healthy", 2, TrustLevel.STANDARD, 8);
        byte[] content = bytes("content that must survive a corrupt source");
        corrupt.publish(content);
        String cid = healthy.publish(content);
        corrupt.setCorruptChunks(true);

        try (FetchGateway gateway = newGateway(new GatewayProperties(), corrupt, healthy)) {
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals("healthy", result.sourceId());
        }
    }

    @Test
    void fallsBackWhenPreferredSourceIsFlaky() throws Exception {
        FakeContentSource flaky = new FakeContentSource("flaky", 1, TrustLevel.STANDARD, 8);
        FakeContentSource backup = new FakeContentSource("backup", 2, TrustLevel.STANDARD, 8);
        byte[] content = bytes("flaky source should not fail the read");
        flaky.publish(content);
        String cid = backup.publish(content);
        flaky.injectErrors(100);

        try (FetchGateway gateway = newGateway(new GatewayProperties(), flaky, backup)) {
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals("backup", result.sourceId());
        }
    }

    @Test
    void failsWhenAllSourcesReturnCorruptContent() {
        FakeContentSource a = new FakeContentSource("a", 1, TrustLevel.STANDARD, 8);
        FakeContentSource b = new FakeContentSource("b", 2, TrustLevel.STANDARD, 8);
        byte[] content = bytes("nobody can serve this correctly");
        a.publish(content);
        String cid = b.publish(content);
        a.setCorruptChunks(true);
        b.setCorruptChunks(true);

        try (FetchGateway gateway = newGateway(new GatewayProperties(), a, b)) {
            assertThrows(FetchException.class, () -> gateway.fetch(cid));
        }
    }

    @Test
    void prefersHigherTrustAmongSamePriority() throws Exception {
        FakeContentSource lowTrust = new FakeContentSource("lowTrust", 1, TrustLevel.LOW, 8);
        FakeContentSource highTrust = new FakeContentSource("highTrust", 1, TrustLevel.HIGH, 8);
        byte[] content = bytes("trust ordering matters");
        lowTrust.publish(content);
        String cid = highTrust.publish(content);

        try (FetchGateway gateway = newGateway(new GatewayProperties(), lowTrust, highTrust)) {
            FetchResult result = gateway.fetch(cid);
            assertEquals("highTrust", result.sourceId());
            assertEquals(0, lowTrust.totalChunkFetches());
        }
    }
}
