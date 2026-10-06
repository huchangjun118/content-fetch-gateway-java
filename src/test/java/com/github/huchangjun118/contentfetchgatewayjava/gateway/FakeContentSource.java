package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地测试替身：可模拟来源抖动、返回损坏内容、慢速响应，
 * 并统计每个分块被请求的次数，用于验证续传不重复下载。
 */
public class FakeContentSource implements ContentSource {

    private final String id;
    private final int priority;
    private final TrustLevel trustLevel;
    private final int chunkSize;

    private volatile byte[] content;
    private volatile ContentManifest manifest;

    /** 剩余注入错误次数：>0 时 fetchChunk 抛出 SourceException。 */
    private final AtomicInteger injectedErrors = new AtomicInteger(0);
    /** 成功服务 N 个分块后抛出一次错误（模拟中途断连），随后自动恢复。 */
    private final AtomicInteger failAfterChunks = new AtomicInteger(-1);
    /** 在该分块处阻塞等待闸门打开（模拟来源卡死）。 */
    private final AtomicInteger blockAtChunk = new AtomicInteger(-1);
    private volatile java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(0);
    /** 返回被篡改的分块（长度不变，内容翻转一字节）。 */
    private volatile boolean corruptChunks = false;
    /** 每个分块的响应延迟。 */
    private volatile long chunkDelayMillis = 0;
    /** 每个分块被成功请求的次数（chunkIndex -> count）。 */
    private final ConcurrentMap<Integer, AtomicInteger> chunkFetchCounts = new ConcurrentHashMap<>();
    private final AtomicInteger manifestCalls = new AtomicInteger(0);

    public FakeContentSource(String id, int priority, TrustLevel trustLevel, int chunkSize) {
        this.id = id;
        this.priority = priority;
        this.trustLevel = trustLevel;
        this.chunkSize = chunkSize;
    }

    /** 设置内容并生成对应的 cid 与清单。 */
    public String publish(byte[] content) {
        this.content = content;
        List<String> chunkHashes = new ArrayList<>();
        for (int i = 0; i < chunkCount(content); i++) {
            chunkHashes.add(Hashes.sha256Hex(chunkOf(content, i)));
        }
        this.manifest = new ContentManifest(Hashes.cidOf(content), content.length, chunkSize, chunkHashes);
        return this.manifest.cid();
    }

    public String cid() {
        return manifest.cid();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public int priority() {
        return priority;
    }

    @Override
    public TrustLevel trustLevel() {
        return trustLevel;
    }

    @Override
    public ContentManifest manifest(String cid) throws SourceException {
        manifestCalls.incrementAndGet();
        if (manifest == null || !manifest.cid().equals(cid)) {
            throw new SourceException(id + ": unknown cid " + cid);
        }
        return manifest;
    }

    @Override
    public byte[] fetchChunk(String cid, int chunkIndex) throws SourceException {
        if (chunkDelayMillis > 0) {
            try {
                Thread.sleep(chunkDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceException(id + ": interrupted");
            }
        }
        if (injectedErrors.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new SourceException(id + ": injected error");
        }
        int remaining = failAfterChunks.get();
        if (remaining == 0) {
            failAfterChunks.set(-1);
            throw new SourceException(id + ": transient disconnect");
        }
        if (remaining > 0) {
            failAfterChunks.decrementAndGet();
        }
        if (manifest == null || !manifest.cid().equals(cid)) {
            throw new SourceException(id + ": unknown cid " + cid);
        }
        if (chunkIndex == blockAtChunk.get()) {
            try {
                gate.await(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceException(id + ": interrupted while blocked");
            }
        }
        chunkFetchCounts.computeIfAbsent(chunkIndex, k -> new AtomicInteger()).incrementAndGet();
        byte[] chunk = chunkOf(content, chunkIndex);
        if (corruptChunks) {
            chunk = chunk.clone();
            chunk[0] ^= 0xFF;
        }
        return chunk;
    }

    // ---- 测试控制与断言辅助 ----

    public void injectErrors(int count) {
        injectedErrors.set(count);
    }

    /** 成功服务 n 个分块后断连一次，随后自动恢复。 */
    public void failAfterChunks(int n) {
        failAfterChunks.set(n);
    }

    /** 在指定分块处阻塞，直到 {@link #openGate()} 被调用。 */
    public void blockAtChunk(int index) {
        gate = new java.util.concurrent.CountDownLatch(1);
        blockAtChunk.set(index);
    }

    public void openGate() {
        blockAtChunk.set(-1);
        gate.countDown();
    }

    public void setCorruptChunks(boolean corrupt) {
        this.corruptChunks = corrupt;
    }

    public void setChunkDelayMillis(long delay) {
        this.chunkDelayMillis = delay;
    }

    public int chunkFetchCount(int chunkIndex) {
        AtomicInteger c = chunkFetchCounts.get(chunkIndex);
        return c == null ? 0 : c.get();
    }

    public int totalChunkFetches() {
        return chunkFetchCounts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    public int manifestCalls() {
        return manifestCalls.get();
    }

    private int chunkCount(byte[] data) {
        return Math.max(1, (data.length + chunkSize - 1) / chunkSize);
    }

    private byte[] chunkOf(byte[] data, int index) {
        int from = index * chunkSize;
        int to = Math.min(data.length, from + chunkSize);
        return Arrays.copyOfRange(data, from, to);
    }
}
