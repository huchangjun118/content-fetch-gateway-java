package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：多读取方并发请求同一内容时请求合并，
 * 重复请求不穿透到来源；慢来源不拖垮缓存已可用的读取。
 */
class ConcurrentFetchTest {

    @TempDir
    Path tempDir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void concurrentReadsAreCoalescedIntoOneBackendFetch() throws Exception {
        FakeContentSource slow = new FakeContentSource("slow", 1, TrustLevel.STANDARD, 16);
        byte[] content = bytes("coalesced content for many concurrent readers!!");
        String cid = slow.publish(content);
        slow.setChunkDelayMillis(50);

        GatewayProperties props = new GatewayProperties();
        props.setFetchTimeout(Duration.ofSeconds(10));
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, slow)) {
            int readers = 8;
            List<CompletableFuture<FetchResult>> futures = new ArrayList<>();
            for (int i = 0; i < readers; i++) {
                futures.add(gateway.fetchAsync(cid));
            }
            for (CompletableFuture<FetchResult> f : futures) {
                FetchResult r = f.get(10, TimeUnit.SECONDS);
                assertArrayEquals(content, r.content());
            }
            int chunkCount = (content.length + 15) / 16;
            assertEquals(chunkCount, slow.totalChunkFetches(),
                    "并发请求应合并为一次回源，每个分块只拉取一次");
        }
    }

    @Test
    void cachedReadIsNotBlockedBySlowInflightFetch() throws Exception {
        FakeContentSource source = new FakeContentSource("s", 1, TrustLevel.STANDARD, 16);
        byte[] cachedContent = bytes("already cached content");
        byte[] slowContent = bytes("slow content that takes a while to download");
        String cachedCid = source.publish(cachedContent);

        GatewayProperties props = new GatewayProperties();
        props.setFetchTimeout(Duration.ofSeconds(10));
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            // 先让 cachedCid 进入缓存
            FetchResult first = gateway.fetch(cachedCid);
            assertFalse(first.fromCache());

            // 发布慢内容并开启延迟
            String slowCid = source.publish(slowContent);
            source.setChunkDelayMillis(200);

            CompletableFuture<FetchResult> slowFuture = gateway.fetchAsync(slowCid);
            // 给慢获取一点进入回源流程的时间
            Thread.sleep(100);

            long start = System.nanoTime();
            FetchResult cached = gateway.fetch(cachedCid);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertTrue(cached.fromCache());
            assertArrayEquals(cachedContent, cached.content());
            assertTrue(elapsedMillis < 1000, "缓存命中不应被慢来源拖垮，耗时 " + elapsedMillis + "ms");
            assertFalse(slowFuture.isDone(), "慢获取此时应仍在进行");

            FetchResult slowResult = slowFuture.get(10, TimeUnit.SECONDS);
            assertArrayEquals(slowContent, slowResult.content());
        }
    }
}
