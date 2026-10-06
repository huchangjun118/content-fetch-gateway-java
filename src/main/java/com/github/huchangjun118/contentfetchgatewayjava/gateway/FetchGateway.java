package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内容获取网关：多来源调度、完整性校验、分块续传、
 * 请求合并、缓存、取消与超时。
 *
 * <p>一致性保证：</p>
 * <ul>
 *   <li>返回的内容一定是来自单一来源、通过分块与整体两级哈希校验的完整副本，
 *       不同来源的片段不会被拼成一份内容；</li>
 *   <li>同一 cid 的并发请求合并为一次实际获取，缓存命中直接返回，不回源；</li>
 *   <li>未完成（取消 / 超时 / 失败）的获取只留在暂存区，对读取方不可见。</li>
 * </ul>
 */
public class FetchGateway implements AutoCloseable {

    private static final String CACHE_SOURCE = "cache";

    private final List<ContentSource> sources;
    private final SourceHealthTracker healthTracker;
    private final ContentCache cache;
    private final StagingStore stagingStore;
    private final GatewayProperties properties;
    private final ExecutorService executor;
    private final ConcurrentMap<String, FetchTask> inFlight = new ConcurrentHashMap<>();

    private static final class FetchTask {
        final CompletableFuture<FetchResult> future = new CompletableFuture<>();
        final AtomicBoolean cancelled = new AtomicBoolean(false);
    }

    public FetchGateway(List<ContentSource> sources,
                        SourceHealthTracker healthTracker,
                        ContentCache cache,
                        StagingStore stagingStore,
                        GatewayProperties properties) {
        this.sources = new ArrayList<>(sources);
        // 调度顺序：优先级升序，同级按可信度降序
        this.sources.sort(Comparator
                .comparingInt(ContentSource::priority)
                .thenComparingInt(s -> s.trustLevel().ordinal()));
        this.healthTracker = healthTracker;
        this.cache = cache;
        this.stagingStore = stagingStore;
        this.properties = properties;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "fetch-gateway-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /** 同步获取（带整体超时，超时后底层获取被取消）。 */
    public FetchResult fetch(String cid) throws FetchException {
        CompletableFuture<FetchResult> future = fetchAsync(cid);
        try {
            return future.get(properties.getFetchTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cancel(cid);
            throw new FetchException("fetch timed out for " + cid, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancel(cid);
            throw new FetchException("fetch interrupted for " + cid, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof FetchException fe) {
                throw fe;
            }
            throw new FetchException("fetch failed for " + cid, cause);
        }
    }

    /**
     * 异步获取；对同一 cid 的并发请求合并为一次实际获取。
     * 缓存命中直接完成，不进入回源流程，慢来源不会拖垮缓存可用的读取。
     */
    public CompletableFuture<FetchResult> fetchAsync(String cid) {
        Optional<ContentCache.CachedContent> hit = cache.get(cid);
        if (hit.isPresent()) {
            ContentCache.CachedContent c = hit.get();
            return CompletableFuture.completedFuture(
                    new FetchResult(cid, c.content(), CACHE_SOURCE, true));
        }
        FetchTask task = inFlight.compute(cid, (k, existing) -> {
            if (existing != null && !existing.future.isDone()) {
                return existing;
            }
            FetchTask created = new FetchTask();
            executor.submit(() -> runFetch(k, created));
            return created;
        });
        return task.future;
    }

    /** 取消进行中的获取；暂存保留以便续传，未完成内容对读取方不可见。 */
    public void cancel(String cid) {
        FetchTask task = inFlight.get(cid);
        if (task != null && task.cancelled.compareAndSet(false, true)) {
            task.future.completeExceptionally(new FetchException("fetch cancelled for " + cid));
        }
    }

    private void runFetch(String cid, FetchTask task) {
        try {
            // 工作线程内再查一次缓存：等待调度期间可能已有其他请求完成并写入
            Optional<ContentCache.CachedContent> hit = cache.get(cid);
            if (hit.isPresent()) {
                ContentCache.CachedContent c = hit.get();
                task.future.complete(new FetchResult(cid, c.content(), CACHE_SOURCE, true));
                return;
            }
            FetchResult result = fetchFromSources(cid, task);
            task.future.complete(result);
        } catch (FetchException e) {
            task.future.completeExceptionally(e);
        } catch (Exception e) {
            task.future.completeExceptionally(new FetchException("fetch failed for " + cid, e));
        } finally {
            inFlight.remove(cid, task);
        }
    }

    private FetchResult fetchFromSources(String cid, FetchTask task) throws FetchException {
        List<String> failures = new ArrayList<>();
        for (ContentSource source : sources) {
            if (task.cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw new FetchException("fetch cancelled for " + cid);
            }
            if (!healthTracker.allow(source.id())) {
                failures.add(source.id() + ": circuit open");
                continue;
            }
            try {
                FetchResult result = fetchFromSource(cid, source, task);
                healthTracker.recordSuccess(source.id());
                return result;
            } catch (CorruptContentException e) {
                healthTracker.recordFailure(source.id(), SourceHealthTracker.FailureKind.CORRUPTION);
                stagingStore.discard(cid, source.id());
                failures.add(source.id() + ": " + e.getMessage());
            } catch (SourceException | IOException e) {
                healthTracker.recordFailure(source.id(), SourceHealthTracker.FailureKind.ERROR);
                failures.add(source.id() + ": " + e.getMessage());
            }
        }
        throw new FetchException("all sources failed for " + cid + ": " + failures);
    }

    /**
     * 从单一来源获取完整内容。分块逐块校验后追加到该来源专属的暂存文件，
     * 中断后从已校验位置续传；全部完成后做整体哈希校验并原子提升进缓存。
     */
    private FetchResult fetchFromSource(String cid, ContentSource source, FetchTask task)
            throws SourceException, IOException, CorruptContentException, FetchException {
        ContentManifest manifest = source.manifest(cid);
        validateManifest(cid, manifest);

        long verified = stagingStore.prepare(cid, source.id(), manifest.chunkHashes());
        int fromChunk = (int) (verified / manifest.chunkSize());

        for (int i = fromChunk; i < manifest.chunkCount(); i++) {
            if (task.cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw new FetchException("fetch cancelled for " + cid);
            }
            byte[] chunk = source.fetchChunk(cid, i);
            verifyChunk(manifest, i, chunk);
            stagingStore.append(cid, source.id(), chunk);
        }

        byte[] content = stagingStore.readAll(cid, source.id());
        if (content.length != manifest.totalSize()
                || !Hashes.sha256Hex(content).equals(Hashes.contentHashOf(cid))) {
            throw new CorruptContentException("overall hash mismatch from " + source.id());
        }

        // 校验全部通过：原子提升进缓存（移动而非重写，不重复落盘）
        cache.put(cid, stagingStore.pathOf(cid, source.id()));
        stagingStore.discardMeta(cid, source.id());
        return new FetchResult(cid, content, source.id(), false);
    }

    private static void validateManifest(String cid, ContentManifest manifest)
            throws CorruptContentException {
        if (manifest.chunkSize() <= 0 || manifest.chunkCount() == 0) {
            throw new CorruptContentException("invalid manifest for " + cid);
        }
        long expected = (long) (manifest.chunkCount() - 1) * manifest.chunkSize() + 1;
        if (manifest.totalSize() < expected
                || manifest.totalSize() > (long) manifest.chunkCount() * manifest.chunkSize()) {
            throw new CorruptContentException("manifest size inconsistent for " + cid);
        }
    }

    private static void verifyChunk(ContentManifest manifest, int index, byte[] chunk)
            throws CorruptContentException {
        long expectedLength = index == manifest.chunkCount() - 1
                ? manifest.totalSize() - (long) index * manifest.chunkSize()
                : manifest.chunkSize();
        if (chunk.length != expectedLength
                || !Hashes.sha256Hex(chunk).equals(manifest.chunkHashes().get(index))) {
            throw new CorruptContentException("chunk " + index + " failed integrity check");
        }
    }

    /** 来源返回了未通过完整性校验的内容。 */
    private static final class CorruptContentException extends Exception {
        CorruptContentException(String message) {
            super(message);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
