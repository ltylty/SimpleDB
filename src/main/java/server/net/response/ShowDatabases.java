package server.net.response;

import java.util.ArrayList;
import java.util.List;

import server.DatabaseInstance;
import server.net.handler.frontend.FrontendConnection;
import server.net.proto.mysql.EOFPacket;
import server.net.proto.mysql.FieldPacket;
import server.net.proto.mysql.ResultSetHeaderPacket;
import server.net.proto.mysql.RowDataPacket;
import server.net.proto.util.Fields;
import server.net.proto.util.PacketUtil;
import server.net.proto.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * ShowDatabases
 *
 * @Author lizhuyang
 */
public final class ShowDatabases {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void response(FrontendConnection c) {
        ChannelHandlerContext ctx = c.getCtx();
        ByteBuf buffer = ctx.alloc().buffer();

        // write header
        buffer = header.writeBuf(buffer, ctx);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.writeBuf(buffer, ctx);
        }

        // write eof
        buffer = eof.writeBuf(buffer, ctx);

        // write rows
        byte packetId = eof.packetId;

        for (String name : getSchemas()) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(name, c.getCharset()));
            row.packetId = ++packetId;
            buffer = row.writeBuf(buffer, ctx);
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, ctx);

        // write buffer
        ctx.writeAndFlush(buffer);
    }

    private static List<String> getSchemas() {
        DatabaseInstance databaseInstance = DatabaseInstance.getInstance();
        ArrayList<String> list = new ArrayList<String>();
        // 当前没有schema概念
        list.add("freedom");
        return list;
    }
}
