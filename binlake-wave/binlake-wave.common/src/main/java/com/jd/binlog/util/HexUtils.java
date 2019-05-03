package com.jd.binlog.util;

/**
 * Created on 18-6-29
 * <p>
 * hex 算法与go hex.go 一致
 *
 * @author pengan
 */
public class HexUtils {
    static final char[] hexTable =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                    'a', 'b', 'c', 'd', 'e', 'f'};

    static void encode(byte[] src, byte[] dst) {
        for (int i = 0; i < src.length; i++) {
            // 获取高 4bit 注意java 往右移 补1
            dst[i * 2] = (byte) hexTable[(src[i] >> 4) & 0x0f];

            // 获取 低 4bit
            dst[i * 2 + 1] = (byte) hexTable[src[i] & 0x0f];
        }
    }

    static int encodeLen(byte[] src) {
        return src.length << 1;
    }

    static int decodeLen(byte[] src) {
        return src.length >> 1;
    }

    static byte fromHexChar(byte c) {
        if (c >= '0' && c <= '9') {
            return (byte) (c - '0');
        }

        if (c >= 'a' && c <= 'f') {
            return (byte) (c - 'a' + 10);
        }

        if (c >= 'A' && c <= 'F') {
            return (byte) (c - 'A' + 10);
        }

        throw new RuntimeException("not hex character value");
    }

    public static String encodeToString(byte[] src) {
        byte[] dst = new byte[encodeLen(src)];
        encode(src, dst);
        return new String(dst);
    }


    /**
     * @param src
     * @return
     */
    public static byte[] decodeString(String src) {
        byte[] srcBt = src.getBytes();
        byte[] dst = new byte[decodeLen(srcBt)];

        if (srcBt.length % 2 == 1) {
            throw new RuntimeException("encoding/hex: odd length hex string");
        }

        for (int i = 0; i < dst.length; i++) {
            byte a = fromHexChar(srcBt[i * 2]);
            byte b = fromHexChar(srcBt[i * 2 + 1]);
            dst[i] = (byte) ((a << 4) | b);
        }
        return dst;
    }
}
