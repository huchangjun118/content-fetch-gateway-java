package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * 来源可信度。可信度用于同优先级来源之间的调度排序，
 * 低可信来源返回的内容仍需通过完整性校验才会被接受。
 */
public enum TrustLevel {
    HIGH,
    STANDARD,
    LOW
}
