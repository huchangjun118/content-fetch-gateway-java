package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double for a content source. Serves chunked, content-addressed data
 * from an in-memory store and can be programmed to be slow, flaky, or to
 * serve corrupt bytes. All calls are counted so tests can assert that
 * verified chunks are never re-downloaded.
 */
public class FakeContentSource implements ContentSource {

    private final String id;
    private final int priority;
    private final double trust;
    private final int chunkSize;
    private final Map<ContentId, byte[]> store;

    /** Per-call artificial latency. */
    public volatile long latencyMillis = 0;
    /** When true, every call fails with a SourceException. */
    public volatile boolean failAlways = false;
    /** When true, served chunk bytes are tampered with. */
    public volatile boolean corruptChunks = false;
    /** What probe() returns while quarantined. */
    public volatile boolean probeResult = true;

    /** Number of upcoming calls (manifest or chunk) that will fail. */
    public final AtomicInteger failNextCalls = new AtomicInteger(0);
    /** Number of upcoming calls that will succeed; afterwards calls fail. */
    public final AtomicInteger successBudget = new AtomicInteger(Integer.MAX_VALUE);
    public final AtomicInteger manifestCalls = new AtomicInteger(0);
    public final AtomicInteger chunkCalls = new AtomicInteger(0);
    public final Map<Integer, AtomicInteger> chunkCallCounts = new ConcurrentHashMap<>();

    public FakeContentSource(String id, int priority, double trust, int chunkSize,
                             Map<ContentId, byte[]> store) {
        this.id = id;
        this.priority = priority;
        this.trust = trust;
        this.chunkSize = chunkSize;
        this.store = store;
    }

    public static Map<ContentId, byte[]> storeOf(byte[]... contents) {
        Map<ContentId, byte[]> store = new ConcurrentHashMap<>();
        for (byte[] content : contents) {
            store.put(ContentId.of(content), content);
        }
        return store;
    }

    public static ContentManifest manifestFor(ContentId id, byte[] content, int chunkSize) {
        List<String> hashes = new ArrayList<>();
        for (int offset = 0; offset < content.length; offset += chunkSize) {
            byte[] chunk = Arrays.copyOfRange(content, offset, Math.min(content.length, offset + chunkSize));
            hashes.add(ContentId.sha256Hex(chunk));
        }
        return new ContentManifest(id, content.length, chunkSize, hashes);
    }

    private void beforeCall() {
        if (latencyMillis > 0) {
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceException(id, "interrupted");
            }
        }
        if (failAlways
                || failNextCalls.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0
                || successBudget.get() <= 0) {
            throw new SourceException(id, "simulated transport failure");
        }
        successBudget.decrementAndGet();
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
    public double trust() {
        return trust;
    }

    @Override
    public ContentManifest manifest(ContentId id) {
        manifestCalls.incrementAndGet();
        beforeCall();
        byte[] content = store.get(id);
        if (content == null) {
            throw new SourceException(this.id, "unknown content " + id);
        }
        return manifestFor(id, content, chunkSize);
    }

    @Override
    public Chunk fetchChunk(ContentId id, int chunkIndex) {
        chunkCalls.incrementAndGet();
        chunkCallCounts.computeIfAbsent(chunkIndex, k -> new AtomicInteger()).incrementAndGet();
        beforeCall();
        byte[] content = store.get(id);
        if (content == null) {
            throw new SourceException(this.id, "unknown content " + id);
        }
        int offset = chunkIndex * chunkSize;
        if (offset >= content.length) {
            throw new SourceException(this.id, "chunk index out of range " + chunkIndex);
        }
        byte[] data = Arrays.copyOfRange(content, offset, Math.min(content.length, offset + chunkSize));
        if (corruptChunks) {
            data = data.clone();
            data[0] ^= 0xFF;
        }
        return new Chunk(chunkIndex, data);
    }

    @Override
    public boolean probe() {
        return probeResult;
    }
}
