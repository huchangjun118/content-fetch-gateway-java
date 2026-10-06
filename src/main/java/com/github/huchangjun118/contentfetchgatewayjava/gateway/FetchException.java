package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * 网关获取失败：所有来源均不可用 / 校验不通过 / 超时 / 被取消。
 */
public class FetchException extends Exception {

    public FetchException(String message) {
        super(message);
    }

    public FetchException(String message, Throwable cause) {
        super(message, cause);
    }
}
