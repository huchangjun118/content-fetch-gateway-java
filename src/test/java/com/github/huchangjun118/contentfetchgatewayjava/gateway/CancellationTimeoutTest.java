package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：获取的取消与超时。未完成的获取不会留下
 * 被后续读取当成可用结果的半份内容；已校验分块保留用于续传。
 */
class CancellationTimeoutTest {

    @TempDir
    Path tempDir;

    private static byte[] contentOfChunks(int chunks, int chunkSize) {
        byte[] data = new byte[chunks * chunkSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('a' + (i / chunkSize) % 26);
        }
        return data;
    }

    private long cacheFileCount() throws Exception {
        try (Stream<Path> files = Files.list(tempDir.resolve("cache"))) {
            return files.count();
        }
    }

    @Test
    void cancelledFetchLeavesNoUsableContentAndCanResume() throws Exception {
        int chunkSize = 16;
        int chunkCount = 8;
        byte[] content = contentOfChunks(chunkCount, chunkSize);

        FakeContentSource source = new FakeContentSource("s", 1, TrustLevel.STANDARD, chunkSize);
        String cid = source.publish(content);
        // 在第 3 个分块处卡死
        source.blockAtChunk(3);

        GatewayProperties props = new GatewayProperties();
        props.setFetchTimeout(Duration.ofSeconds(10));
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            CompletableFuture<FetchResult> future = gateway.fetchAsync(cid);

            // 等待前 3 个分块完成校验并落盘
            long deadline = System.currentTimeMillis() + 5000;
            while (source.chunkFetchCount(2) < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, source.chunkFetchCount(2));

            gateway.cancel(cid);
            source.openGate();
            assertThrows(ExecutionException.class, future::get);

            // 未完成的获取不产生任何可读缓存
            assertEquals(0, cacheFileCount(), "取消后不得留下可用半份内容");

            // 重新获取：从已校验位置续传，已校验分块不重复下载
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            for (int i = 0; i < 3; i++) {
                assertEquals(1, source.chunkFetchCount(i),
                        "取消前已校验的分块 " + i + " 不得重复下载");
            }
            assertEquals(content.length, result.content().length);
        }
    }

    @Test
    void timedOutFetchLeavesNoUsableContent() throws Exception {
        int chunkSize = 16;
        byte[] content = contentOfChunks(6, chunkSize);

        FakeContentSource source = new FakeContentSource("slow", 1, TrustLevel.STANDARD, chunkSize);
        String cid = source.publish(content);
        source.setChunkDelayMillis(300);

        GatewayProperties props = new GatewayProperties();
        props.setFetchTimeout(Duration.ofMillis(400));
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            assertThrows(FetchException.class, () -> gateway.fetch(cid));
            assertEquals(0, cacheFileCount(), "超时后不得留下可用半份内容");

            // 来源恢复后重新获取成功，且利用了已校验的暂存分块
            source.setChunkDelayMillis(0);
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertTrue(source.chunkFetchCount(0) <= 1, "已校验分块不得重复下载");
        }
    }
}
