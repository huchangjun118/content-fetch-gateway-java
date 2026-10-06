package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * 一次成功读取的结果。
 *
 * @param cid       内容地址
 * @param content   完整内容字节（已通过完整性校验）
 * @param sourceId  实际提供内容的来源；缓存命中时为 "cache"
 * @param fromCache 是否来自缓存
 */
public record FetchResult(String cid, byte[] content, String sourceId, boolean fromCache) {
}
