package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * A source returned bytes that failed integrity verification.
 */
public class CorruptContentException extends SourceException {

    public CorruptContentException(String sourceId, String message) {
        super(sourceId, message);
    }
}
