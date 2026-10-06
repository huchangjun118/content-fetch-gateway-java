package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * A source failed to serve a request (network, timeout, unavailable...).
 */
public class SourceException extends RuntimeException {

    private final String sourceId;

    public SourceException(String sourceId, String message) {
        super(message);
        this.sourceId = sourceId;
    }

    public SourceException(String sourceId, String message, Throwable cause) {
        super(message, cause);
        this.sourceId = sourceId;
    }

    public String sourceId() {
        return sourceId;
    }
}
