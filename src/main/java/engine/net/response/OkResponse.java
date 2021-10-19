package engine.net.response;

import engine.net.handler.frontend.FrontendConnection;
import engine.net.proto.mysql.OkPacket;
import io.netty.channel.ChannelHandlerContext;

/**
 * OkResponse
 *
 * @Author lizhuyang
 */
public class OkResponse {
    public static void response(FrontendConnection c) {
        OkPacket okPacket = new OkPacket();
        ChannelHandlerContext ctx = c.getCtx();
        okPacket.write(ctx);
    }

    public static void responseWithAffectedRows(FrontendConnection c, long affectedRows) {
        OkPacket okPacket = new OkPacket();
        okPacket.affectedRows = affectedRows;
        ChannelHandlerContext ctx = c.getCtx();
        okPacket.write(ctx);
    }
}
