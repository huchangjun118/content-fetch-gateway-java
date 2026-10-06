package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * 已校验内容的磁盘缓存，带过期策略。
 *
 * <p>一致性语义：</p>
 * <ul>
 *   <li>内容是内容寻址的，读取时重新校验内容哈希，损坏条目自动清除（自愈）
 *       并视为未命中，触发回源；</li>
 *   <li>条目写入后不可变，淘汰只删除文件，不影响已经拿到字节副本的读取方；</li>
 *   <li>过期条目不返回，保证不会无限期提供与来源当前内容不一致的结果。</li>
 * </ul>
 */
public class ContentCache {

    /**
     * @param cid       内容地址
     * @param content   内容字节
     * @param fetchedAt 写入时间（epoch millis）
     */
    public record CachedContent(String cid, byte[] content, long fetchedAt) {
    }

    private final Path cacheDir;
    private final long ttlMillis;

    public ContentCache(Path cacheDir, long ttlMillis) {
        this.cacheDir = cacheDir;
        this.ttlMillis = ttlMillis;
        try {
            Files.createDirectories(cacheDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create cache dir " + cacheDir, e);
        }
    }

    /** 命中且未过期且校验通过时返回内容；损坏或过期返回空并清除条目。 */
    public Optional<CachedContent> get(String cid) {
        Path p = pathOf(cid);
        try {
            if (!Files.exists(p)) {
                return Optional.empty();
            }
            long fetchedAt = Files.getLastModifiedTime(p).toMillis();
            if (System.currentTimeMillis() - fetchedAt > ttlMillis) {
                evict(cid);
                return Optional.empty();
            }
            byte[] content = Files.readAllBytes(p);
            if (!Hashes.sha256Hex(content).equals(Hashes.contentHashOf(cid))) {
                // 缓存损坏：自愈清除，按未命中处理
                evict(cid);
                return Optional.empty();
            }
            return Optional.of(new CachedContent(cid, content, fetchedAt));
        } catch (IOException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /** 将已完成校验的暂存文件原子移入缓存（不重复落盘）。 */
    public void put(String cid, Path stagingFile) {
        try {
            Files.move(stagingFile, pathOf(cid),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // 某些文件系统不支持原子移动，退化为普通移动
            try {
                Files.move(stagingFile, pathOf(cid), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new UncheckedIOException("cannot promote staging to cache for " + cid, ex);
            }
        }
    }

    public void evict(String cid) {
        try {
            Files.deleteIfExists(pathOf(cid));
        } catch (IOException ignored) {
            // 清理失败不影响主流程
        }
    }

    private Path pathOf(String cid) {
        return cacheDir.resolve(cid.replaceAll("[^A-Za-z0-9._-]", "_") + ".bin");
    }
}
