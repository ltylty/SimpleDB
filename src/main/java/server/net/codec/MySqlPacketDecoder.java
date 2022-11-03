package server.net.codec;

import io.netty.buffer.ByteBufUtil;
import server.net.proto.mysql.BinaryPacket;
import server.net.proto.util.ByteUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * MySqlPacketDecoder
 *
 * @Author lizhuyang
 */
public class MySqlPacketDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(MySqlPacketDecoder.class);

    private final int packetHeaderSize = 4;
    private final int maxPacketSize = 16 * 1024 * 1024;

    /**
     * MySql外层结构解包
     *
     * @param ctx
     * @param in
     * @param out
     *
     * @throws Exception
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 防粘包、半包
        // 4 bytes:3 length + 1 packetId
        if (in.readableBytes() < packetHeaderSize) {
            return;
        }
        in.markReaderIndex();
        int packetLength = ByteUtil.readUB3(in);
        // 过载保护
        if (packetLength > maxPacketSize) {
            throw new IllegalArgumentException("Packet size over the limit " + maxPacketSize);
        }
        byte packetId = in.readByte();
        if (in.readableBytes() < packetLength) {
            // 半包回溯
            in.resetReaderIndex();
            return;
        }
        BinaryPacket packet = new BinaryPacket();
        packet.packetLength = packetLength;
        packet.packetId = packetId;
        // data will not be accessed any more,so we can use this array safely
        //packet.data = in.readBytes(packetLength).array();
        byte[] bytes = new byte[packetLength];
        in.readBytes(bytes);
        packet.data = bytes;
        if (packet.data == null || packet.data.length == 0) {
            logger.error("getDecoder data errorMessage,packetLength=" + packet.packetLength);
        }
        out.add(packet);
    }
}
