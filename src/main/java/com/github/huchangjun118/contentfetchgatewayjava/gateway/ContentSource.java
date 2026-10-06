package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * A content provider. Implementations fetch content-addressed data; the
 * gateway never trusts returned bytes until they verify against the manifest.
 */
public interface ContentSource {

    /** Stable identifier of this source. */
    String id();

    /** Lower value = preferred when several sources are available. */
    int priority();

    /**
     * Declared trust weight in [0,1]. Corrupt data from a high-trust source
     * penalises its health score more.
     */
    default double trust() {
        return 1.0;
    }

    /** Fetch the manifest describing the content. */
    ContentManifest manifest(ContentId id) throws SourceException;

    /** Fetch one chunk. Offsets are derived from the manifest chunk size. */
    Chunk fetchChunk(ContentId id, int chunkIndex) throws SourceException;

    /** Liveness probe used to leave quarantine. */
    default boolean probe() {
        return true;
    }
}
