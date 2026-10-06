package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.List;

/**
 * 内容清单：描述一份内容寻址数据的分块结构。
 * cid 形如 "sha256:<hex>"，为完整内容的哈希。
 *
 * @param cid         内容地址（完整内容哈希）
 * @param totalSize   内容总字节数
 * @param chunkSize   分块大小（最后一块可能更小）
 * @param chunkHashes 每个分块的 sha256 哈希（hex），用于分块级完整性校验
 */
public record ContentManifest(String cid, long totalSize, int chunkSize, List<String> chunkHashes) {

    public ContentManifest {
        chunkHashes = List.copyOf(chunkHashes);
    }

    public int chunkCount() {
        return chunkHashes.size();
    }
}
