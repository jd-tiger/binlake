package com.jd.binlog.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class PacketUtils {

    public static HeaderPacket readHeader(SocketChannel ch, int len) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.read(readBytesAsBuffer(ch, len).array());
        return header;
    }

    public static ByteBuffer readBytesAsBuffer(SocketChannel ch, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(len);
        while (buffer.hasRemaining()) {
            int readNum = ch.read(buffer);
            if (readNum == -1) {
                throw new IOException("Unexpected End Stream");
            }
        }
        return buffer;
    }

    public static byte[] readBytes(SocketChannel ch, int len) throws IOException {
        return readBytesAsBuffer(ch, len).array();
    }

    /**
     * Since We r using blocking IO, so we will just write once and assert the length to simplify the read operation.<br>
     * If the block write doesn't work as we expected, we will change this implementation as per the result.
     *
     * @param ch
     * @return
     * @throws IOException
     */
    public static void write(SocketChannel ch, ByteBuffer[] srcs) throws IOException {
        @SuppressWarnings("unused")
        long total = 0;
        for (ByteBuffer buffer : srcs) {
            total += buffer.remaining();
        }

        ch.write(srcs);
        // https://github.com/alibaba/canal/issues/24
        // 部分windows用户会出现size != total的情况，jdk为java7/openjdk，估计和java版本有关，暂时不做检查
        // long size = ch.write(srcs);
        // if (size != total) {
        // throw new IOException("unexpected blocking io behavior");
        // }
    }

    public static void write(SocketChannel ch, byte[] body) throws IOException {
        write(ch, body, (byte) 0);
    }

    public static void write(SocketChannel ch, byte[] body, byte packetId) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.packetLength = body.length;
        header.packetId = packetId;
        write(ch, new ByteBuffer[]{ByteBuffer.wrap(header.getBytes()), ByteBuffer.wrap(body)});
    }
}
