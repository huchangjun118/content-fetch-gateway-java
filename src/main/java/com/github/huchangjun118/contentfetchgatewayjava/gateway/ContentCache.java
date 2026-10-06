package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.Optional;

/**
 * Stores verified chunks and fully assembled, verified content. Entries expire
 * after a TTL; anything served from here must re-verify before use.
 */
public interface ContentCache {

    /** A previously verified chunk, if present, fresh and still intact. */
    Optional<byte[]> getVerifiedChunk(ContentId id, ContentManifest manifest, int chunkIndex);

    /** Store a chunk that has passed verification. */
    void putChunk(ContentId id, int chunkIndex, byte[] data);

    /** A fully assembled and verified content, if present and fresh. */
    Optional<byte[]> getContent(ContentId id);

    /** Publish a fully verified content. Never call with partial data. */
    void putContent(ContentId id, byte[] content);

    /** Drop everything cached for this content (self-healing). */
    void evict(ContentId id);
}
