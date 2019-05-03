package com.jd.binlog.util;

/**
 * Created by pengan on 17-1-16.
 */
public class UUIDUtils {
    private static final int HEX = 16;
    private static final int UUID_LENGTH = 16;
    private static final String UUID_SEPARATOR = "-";

    private static final Integer[] UUID_PART_LENGTH = {4, 2, 2, 2, 6};

    /**
     * uuid to bytes
     *
     * @param uuid: uuid which include -
     * @return
     */
    public static byte[] UUIDToHexBytes(String uuid) {
        if (uuid.length() < (UUID_LENGTH << 1)) {
            return null;
        }
        String uid = uuid.replace(UUID_SEPARATOR, "");
        byte[] bytes = new byte[UUID_LENGTH];
        char[] array = uid.toCharArray();
        StringBuilder str = new StringBuilder();
        for (int index = 0; index < UUID_LENGTH; index++) {
            char pre = array[2 * index];
            char suf = array[2 * index + 1];
            str.append(pre).append(suf);
            // radio to Hex
            int rst = Integer.parseInt(str.toString(), HEX);

            str.setLength(0);
            bytes[index] = (byte) (rst & 0xff);
        }
        str.setLength(0);
        return bytes;
    }

    /**
     * hex bytes to uuid string which include -
     *
     * @param bytes
     * @return
     */
    public static String hexBytesToUUID(byte[] bytes) {
        if (bytes.length != UUID_LENGTH) {
            return null;
        }
        StringBuilder uuid = new StringBuilder();

        int start = 0;
        for (Integer part : UUID_PART_LENGTH) {
            for (int index = start; index < start + part; index++) {
                uuid.append(String.format("%02x", (bytes[index] & 0xff)));
            }
            uuid.append(UUID_SEPARATOR);
            start += part;
        }

        uuid.setLength(UUID_LENGTH * 2 + 4);// remove last UUID_SEPARATOR
        return uuid.toString();
    }
}
