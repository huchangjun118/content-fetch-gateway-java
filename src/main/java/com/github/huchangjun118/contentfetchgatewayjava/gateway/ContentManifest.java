package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.List;
import java.util.Objects;

/**
 * Describes how a content is split into chunks and the expected hash of each
 * chunk. The manifest itself is addressed by {@link ContentId}, so trusting the
 * address is enough to trust every chunk hash inside.
 */
public class ContentManifest {

    private final ContentId id;
    private final long size;
    private final int chunkSize;
    private final List<String> chunkHashes;

    public ContentManifest(ContentId id, long size, int chunkSize, List<String> chunkHashes) {
        this.id = Objects.requireNonNull(id, "id");
        this.size = size;
        this.chunkSize = chunkSize;
        this.chunkHashes = List.copyOf(chunkHashes);
    }

    public ContentId id() {
        return id;
    }

    public long size() {
        return size;
    }

    public int chunkSize() {
        return chunkSize;
    }

    public int chunkCount() {
        return chunkHashes.size();
    }

    public String expectedChunkHash(int index) {
        return chunkHashes.get(index);
    }

    /** Expected length of a chunk; the last chunk may be shorter. */
    public int expectedChunkLength(int index) {
        long remaining = size - (long) index * chunkSize;
        return (int) Math.min(chunkSize, remaining);
    }

    /** Verify a single chunk against this manifest. */
    public boolean verifyChunk(int index, byte[] data) {
        if (index < 0 || index >= chunkHashes.size()) {
            return false;
        }
        if (data == null || data.length != expectedChunkLength(index)) {
            return false;
        }
        return ContentId.sha256Hex(data).equals(chunkHashes.get(index));
    }

    /** Verify a fully assembled content against the content address. */
    public boolean verifyContent(byte[] content) {
        return content != null
                && content.length == size
                && ContentId.of(content).equals(id);
    }
}
