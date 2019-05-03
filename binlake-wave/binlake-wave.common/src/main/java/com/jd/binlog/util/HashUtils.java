package com.jd.binlog.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * Created on 18-5-16
 *
 * @author pengan
 * @referenced from
 * <a>https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/util/MurmurHash3.java
 * </a>
 */
public class HashUtils {

    private static final int seed = 0xEE6B27EB;

    public static long hash(byte[] key) {
        return hash(key, seed);
    }

    /**
     * murmur-hash 3
     *
     * @param key
     * @param seed
     * @return
     */
    public static long hash(byte[] key, int seed) {

        final ByteBuffer data =
                ByteBuffer.wrap(key).order(ByteOrder.LITTLE_ENDIAN);

        final int len = key.length;

        final int nblocks = len / 16;

        long h1 = seed;
        long h2 = seed;

        final long c1 = 0x87c37b91114253d5L;
        final long c2 = 0x4cf5ad432745937fL;

        final LongBuffer blocks = data.asLongBuffer();

        for (int i = 0; i < nblocks; ++i) {
            long k1 = blocks.get(i * 2 + 0);
            long k2 = blocks.get(i * 2 + 1);

            k1 *= c1;
            k1 = k1 << 31 | k1 >>> (64 - 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = h1 << 27 | h1 >>> (64 - 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = k2 << 33 | k2 >>> (64 - 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = h2 << 31 | h1 >>> (64 - 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        data.position(nblocks * 16);
        final ByteBuffer tail = data.slice();

        long k1 = 0;
        long k2 = 0;

        switch (len & 15) {
            case 15:
                k2 ^= ((long) tail.get(14) & 0xff) << 48;
            case 14:
                k2 ^= ((long) tail.get(13) & 0xff) << 40;
            case 13:
                k2 ^= ((long) tail.get(12) & 0xff) << 32;
            case 12:
                k2 ^= ((long) tail.get(11) & 0xff) << 24;
            case 11:
                k2 ^= ((long) tail.get(10) & 0xff) << 16;
            case 10:
                k2 ^= ((long) tail.get(9) & 0xff) << 8;
            case 9:
                k2 ^= ((long) tail.get(8) & 0xff) << 0;
                k2 *= c2;
                k2 = k2 << 33 | k2 >>> (64 - 33);
                k2 *= c1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) tail.get(7) & 0xff) << 56;
            case 7:
                k1 ^= ((long) tail.get(6) & 0xff) << 48;
            case 6:
                k1 ^= ((long) tail.get(5) & 0xff) << 40;
            case 5:
                k1 ^= ((long) tail.get(4) & 0xff) << 32;
            case 4:
                k1 ^= ((long) tail.get(3) & 0xff) << 24;
            case 3:
                k1 ^= ((long) tail.get(2) & 0xff) << 16;
            case 2:
                k1 ^= ((long) tail.get(1) & 0xff) << 8;
            case 1:
                k1 ^= ((long) tail.get(0) & 0xff) << 0;
                k1 *= c1;
                k1 = k1 << 31 | k1 >>> (64 - 31);
                k1 *= c2;
                h1 ^= k1;
            case 0:
                break;
        }

        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        // h2 += h1;

        return h1;
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
