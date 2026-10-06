package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 下载中内容的暂存区。按 (cid, sourceId) 隔离：
 * 一份内容的暂存只追加同一来源的分块，不同来源的分块绝不混合。
 * 暂存内容在完成整体校验前对读取方不可见，因此中断或取消
 * 不会留下被当成可用结果的半份内容；已校验分块保留在磁盘上，
 * 续传时直接从已校验位置继续，不重复下载、不重复落盘。
 */
public class StagingStore {

    private final Path stagingDir;

    public StagingStore(Path stagingDir) {
        this.stagingDir = stagingDir;
        try {
            Files.createDirectories(stagingDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create staging dir " + stagingDir, e);
        }
    }

    /**
     * 续传准备：校验暂存对应的清单与当前清单一致，返回已校验通过的字节数。
     * 清单不一致时丢弃旧暂存并从零开始。
     */
    public synchronized long prepare(String cid, String sourceId, List<String> chunkHashes)
            throws IOException {
        Path meta = metaPath(cid, sourceId);
        String expected = String.join(",", chunkHashes);
        if (Files.exists(meta) && !Files.readString(meta).equals(expected)) {
            discard(cid, sourceId);
        }
        if (!Files.exists(meta)) {
            Files.writeString(meta, expected);
        }
        return verifiedLength(cid, sourceId);
    }

    /** 已校验通过的字节数（续传起点）。 */
    public synchronized long verifiedLength(String cid, String sourceId) {
        Path p = pathOf(cid, sourceId);
        try {
            return Files.exists(p) ? Files.size(p) : 0;
        } catch (IOException e) {
            return 0;
        }
    }

    /** 追加一个已校验分块。 */
    public synchronized void append(String cid, String sourceId, byte[] chunk) throws IOException {
        Files.write(pathOf(cid, sourceId), chunk,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** 读取暂存内容（用于最终整体校验）。 */
    public synchronized byte[] readAll(String cid, String sourceId) throws IOException {
        Path p = pathOf(cid, sourceId);
        return Files.exists(p) ? Files.readAllBytes(p) : new byte[0];
    }

    /** 暂存文件路径（用于完成后原子移入缓存）。 */
    public Path pathOf(String cid, String sourceId) {
        return stagingDir.resolve(sanitize(cid) + "." + sanitize(sourceId) + ".part");
    }

    /** 暂存提升为缓存后清理清单元数据。 */
    public synchronized void discardMeta(String cid, String sourceId) {
        try {
            Files.deleteIfExists(metaPath(cid, sourceId));
        } catch (IOException ignored) {
            // 清理失败不影响主流程
        }
    }

    /** 丢弃暂存（来源返回损坏内容或校验失败时调用）。 */
    public synchronized void discard(String cid, String sourceId) {
        try {
            Files.deleteIfExists(pathOf(cid, sourceId));
            Files.deleteIfExists(metaPath(cid, sourceId));
        } catch (IOException ignored) {
            // 清理失败不影响主流程
        }
    }

    private Path metaPath(String cid, String sourceId) {
        return stagingDir.resolve(sanitize(cid) + "." + sanitize(sourceId) + ".meta");
    }

    private static String sanitize(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
