package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Content address: hex-encoded SHA-256 of the content.
 */
public record ContentId(String hex) {

    public ContentId {
        Objects.requireNonNull(hex, "hex");
    }

    public static ContentId of(byte[] content) {
        return new ContentId(sha256Hex(content));
    }

    public static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    @Override
    public String toString() {
        return hex;
    }
}
