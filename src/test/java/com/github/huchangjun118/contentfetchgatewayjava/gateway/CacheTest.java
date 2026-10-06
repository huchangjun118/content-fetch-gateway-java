package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：缓存过期回源、缓存损坏自愈、自愈不影响已拿到的读取结果。
 */
class CacheTest {

    @TempDir
    Path tempDir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void expiredCacheEntryIsRefetchedFromSource() throws Exception {
        FakeContentSource source = new FakeContentSource("s", 1, TrustLevel.STANDARD, 16);
        byte[] content = bytes("content with a short cache lifetime");
        String cid = source.publish(content);

        GatewayProperties props = new GatewayProperties();
        props.setCacheTtl(Duration.ofMillis(150));
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            FetchResult first = gateway.fetch(cid);
            assertFalse(first.fromCache());
            int fetchesAfterFirst = source.totalChunkFetches();

            FetchResult second = gateway.fetch(cid);
            assertTrue(second.fromCache(), "未过期应命中缓存");
            assertEquals(fetchesAfterFirst, source.totalChunkFetches(), "缓存命中不应回源");

            Thread.sleep(200);

            FetchResult third = gateway.fetch(cid);
            assertFalse(third.fromCache(), "过期后应回源重新获取");
            assertArrayEquals(content, third.content());
            assertTrue(source.totalChunkFetches() > fetchesAfterFirst, "过期后应重新回源");
        }
    }

    @Test
    void corruptedCacheEntrySelfHeals() throws Exception {
        FakeContentSource source = new FakeContentSource("s", 1, TrustLevel.STANDARD, 16);
        byte[] content = bytes("content whose cache copy gets corrupted");
        String cid = source.publish(content);

        GatewayProperties props = new GatewayProperties();
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            FetchResult first = gateway.fetch(cid);
            assertFalse(first.fromCache());

            // 篡改缓存文件，模拟缓存损坏
            Path cacheDir = tempDir.resolve("cache");
            Path cacheFile;
            try (Stream<Path> files = Files.list(cacheDir)) {
                cacheFile = files.findFirst().orElseThrow();
            }
            Files.write(cacheFile, bytes("tampered-bytes"));

            // 读取时校验发现损坏，自动清除并回源，结果仍然正确
            FetchResult healed = gateway.fetch(cid);
            assertFalse(healed.fromCache());
            assertArrayEquals(content, healed.content());

            // 自愈后缓存恢复可用
            FetchResult again = gateway.fetch(cid);
            assertTrue(again.fromCache());
            assertArrayEquals(content, again.content());
        }
    }

    @Test
    void selfHealDoesNotAffectReadersHoldingPreviousCopy() throws Exception {
        FakeContentSource source = new FakeContentSource("s", 1, TrustLevel.STANDARD, 16);
        byte[] content = bytes("readers keep their consistent snapshot");
        String cid = source.publish(content);

        GatewayProperties props = new GatewayProperties();
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            gateway.fetch(cid);
            FetchResult holder = gateway.fetch(cid);
            assertTrue(holder.fromCache());

            // 损坏缓存并触发自愈
            Path cacheDir = tempDir.resolve("cache");
            try (Stream<Path> files = Files.list(cacheDir)) {
                Files.write(files.findFirst().orElseThrow(), bytes("garbage"));
            }
            FetchResult healed = gateway.fetch(cid);
            assertArrayEquals(content, healed.content());

            // 之前已经拿到结果的读取方不受影响
            assertArrayEquals(content, holder.content());
        }
    }
}
