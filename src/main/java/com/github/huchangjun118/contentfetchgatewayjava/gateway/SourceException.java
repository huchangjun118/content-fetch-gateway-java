package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * 来源访问异常：网络抖动、来源不可用等。
 */
public class SourceException extends Exception {

    public SourceException(String message) {
        super(message);
    }

    public SourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
