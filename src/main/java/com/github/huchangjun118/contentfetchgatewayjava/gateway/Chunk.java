package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * A single chunk of content as transported from a source.
 */
public record Chunk(int index, byte[] data) {
}
