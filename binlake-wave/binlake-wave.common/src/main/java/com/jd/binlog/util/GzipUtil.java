package com.jd.binlog.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by ninet on 19-1-30.
 */
public class GzipUtil {
    private static final int TINYINT_MAX_VALUE = 256;

    public static byte[] compress(byte[] bytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;

        gzip = new GZIPOutputStream(out);
        gzip.write(bytes);
        gzip.close();

        return out.toByteArray();
    }

    public static byte[] uncompress(byte[] bytes) throws Exception {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        GZIPInputStream ungzip = new GZIPInputStream(in);
        byte[] buffer = new byte[TINYINT_MAX_VALUE];
        int num;
        while ((num = ungzip.read(buffer)) >= 0) {
            out.write(buffer, 0, num);
        }
        return out.toByteArray();
    }
}
