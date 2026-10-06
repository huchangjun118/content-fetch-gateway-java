package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 内容寻址使用的哈希工具。cid 形如 "sha256:<hex>"。
 */
public final class Hashes {

    private Hashes() {
    }

    public static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return toHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /** 从 cid 中解析出内容哈希（hex）。 */
    public static String contentHashOf(String cid) {
        if (cid == null || !cid.startsWith("sha256:")) {
            throw new IllegalArgumentException("unsupported cid: " + cid);
        }
        return cid.substring("sha256:".length());
    }

    public static String cidOf(byte[] content) {
        return "sha256:" + sha256Hex(content);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
