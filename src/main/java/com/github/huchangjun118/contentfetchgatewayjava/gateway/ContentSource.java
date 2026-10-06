package com.github.huchangjun118.contentfetchgatewayjava.gateway;

/**
 * 内容来源抽象。实现方可以对接 HTTP、P2P 或本地测试替身。
 */
public interface ContentSource {

    /** 来源唯一标识。 */
    String id();

    /** 优先级，数值越小越优先被调度。 */
    int priority();

    /** 来源声明的可信度。 */
    TrustLevel trustLevel();

    /** 获取内容清单（分块结构）。 */
    ContentManifest manifest(String cid) throws SourceException;

    /** 按分块序号拉取一个分块的完整字节。 */
    byte[] fetchChunk(String cid, int chunkIndex) throws SourceException;
}
