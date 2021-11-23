package server.net.response;

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

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lizhuyang
 */
public class SelectResponse {

    private Integer fieldCount;
    private ResultSetHeaderPacket header;
    private ArrayList<Field> fields;
    private ArrayList<ArrayList<String>> rows;
    private static final EOFPacket eof = new EOFPacket();
    private byte packetId;
    private String originCharset;

    public SelectResponse(Integer fieldCount) {
        this.fieldCount = fieldCount;
        header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        fields = new ArrayList<Field>();
        rows = new ArrayList<ArrayList<String>>();
    }

    public void addField(String fieldName, int type) {
        Field field = new Field(fieldName, type);
        fields.add(field);
    }

    public void responseFields(FrontendConnection c, ByteBuf buffer) {
        buffer = header.writeBuf(buffer, c.getCtx());
        for (Field field : fields) {
            FieldPacket packet = null;
            if (field.getType() == Fields.FIELD_TYPE_INT24) {
                packet = PacketUtil.getField(field.getFieldName(), Fields.FIELD_TYPE_INT24);
            } else {
                packet = PacketUtil.getField(field.getFieldName(), Fields.FIELD_TYPE_VAR_STRING);
            }
            packet.packetId = ++packetId;
            buffer = packet.writeBuf(buffer, c.getCtx());
        }
    }

    public void writeEof(FrontendConnection c, ByteBuf buffer) {
        eof.packetId = ++packetId;
        eof.writeBuf(buffer, c.getCtx());
    }

    public void writeLastEof(FrontendConnection c, ByteBuf buffer) {
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, c.getCtx());
        c.getCtx().writeAndFlush(buffer);
    }

    public void writeRow(List<String> values, FrontendConnection c, ByteBuf buffer) {
        RowDataPacket row = new RowDataPacket(fieldCount);
        for (String item : values) {
            row.add(StringUtil.encode(item, c.getCharset()));
        }
        row.packetId = ++packetId;
        row.writeBuf(buffer, c.getCtx());
    }

    public void response(FrontendConnection c) {
        ChannelHandlerContext ctx = c.getCtx();
        ByteBuf buffer = ctx.alloc().buffer();
        responseFields(c, buffer);
        writeEof(c, buffer);
        for (ArrayList<String> item : rows) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            for (String value : item) {
                // 如果两个charset一样,则无需decode
                row.add(StringUtil.encode(value, c.getCharset()));
            }
            row.packetId = ++packetId;
            buffer = row.writeBuf(buffer, ctx);
        }
        writeLastEof(c, buffer);
    }

    public ArrayList<ArrayList<String>> getRows() {
        return rows;
    }

    public void setRows(ArrayList<ArrayList<String>> rows) {
        this.rows = rows;
    }

    private class Field {
        private String fieldName;
        private int type;

        public Field(String fieldName, int type) {
            this.fieldName = fieldName;
            this.type = type;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    public String getOriginCharset() {
        return originCharset;
    }

    public void setOriginCharset(String originCharset) {
        this.originCharset = originCharset;
    }
}
