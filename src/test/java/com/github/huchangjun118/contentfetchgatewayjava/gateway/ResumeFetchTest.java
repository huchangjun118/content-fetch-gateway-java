package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景：获取中断后从已校验通过的位置续传，
 * 已校验分块不重复下载、不重复落盘。
 */
class ResumeFetchTest {

    @TempDir
    Path tempDir;

    private static byte[] contentOfChunks(int chunks, int chunkSize) {
        byte[] data = new byte[chunks * chunkSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('A' + (i / chunkSize) % 26);
        }
        return data;
    }

    @Test
    void resumesFromVerifiedPositionWithoutRedownloading() throws Exception {
        int chunkSize = 16;
        int chunkCount = 10;
        byte[] content = contentOfChunks(chunkCount, chunkSize);

        FakeContentSource source = new FakeContentSource("s1", 1, TrustLevel.STANDARD, chunkSize);
        String cid = source.publish(content);
        // 成功服务 4 个分块后断连一次
        source.failAfterChunks(4);

        GatewayProperties props = new GatewayProperties();
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, source)) {
            // 第一次获取在中途失败
            assertThrows(FetchException.class, () -> gateway.fetch(cid));
            assertEquals(4, source.totalChunkFetches(), "断连前应已下载 4 个分块");

            // 第二次获取从已校验位置续传并成功
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals("s1", result.sourceId());

            for (int i = 0; i < 4; i++) {
                assertEquals(1, source.chunkFetchCount(i),
                        "已校验分块 " + i + " 不得重复下载");
            }
            for (int i = 4; i < chunkCount; i++) {
                assertEquals(1, source.chunkFetchCount(i),
                        "续传分块 " + i + " 只应下载一次");
            }
        }

        // 完成后暂存区不残留 .part 文件（已原子提升进缓存，不重复落盘）
        try (Stream<Path> files = Files.list(tempDir.resolve("staging"))) {
            assertTrue(files.noneMatch(p -> p.toString().endsWith(".part")),
                    "完成后暂存区不应残留半份内容");
        }
        // 缓存中应存在完整内容
        try (Stream<Path> files = Files.list(tempDir.resolve("cache"))) {
            assertEquals(1, files.count());
        }
    }

    @Test
    void doesNotMixFragmentsFromDifferentSources() throws Exception {
        int chunkSize = 16;
        byte[] content = contentOfChunks(8, chunkSize);

        FakeContentSource s1 = new FakeContentSource("s1", 1, TrustLevel.STANDARD, chunkSize);
        FakeContentSource s2 = new FakeContentSource("s2", 2, TrustLevel.STANDARD, chunkSize);
        s1.publish(content);
        String cid = s2.publish(content);
        // s1 服务 3 个分块后断连，网关在同一次获取中切换到 s2
        s1.failAfterChunks(3);

        GatewayProperties props = new GatewayProperties();
        try (FetchGateway gateway = Gateways.newGateway(tempDir, props, s1, s2)) {
            FetchResult result = gateway.fetch(cid);
            assertArrayEquals(content, result.content());
            assertEquals("s2", result.sourceId());
            assertEquals(3, s1.totalChunkFetches());
            // s2 必须从头提供全部分块，而不是从 s1 已下载的位置拼接
            for (int i = 0; i < 8; i++) {
                assertEquals(1, s2.chunkFetchCount(i),
                        "s2 应提供分块 " + i + "，不得与 s1 的片段拼接");
            }
        }
    }
}
