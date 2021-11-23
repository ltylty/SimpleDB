package server.sql;

import server.net.handler.frontend.FrontendConnection;
import server.net.response.SelectResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import engine.TupleDesc;

import java.util.function.Function;

/**
 * @Author: 刘天元
 * @Date: 2021/10/19 16:53
 */

public class WriteResultBefore implements Function<TupleDesc, WriteResultBO> {

    FrontendConnection con;

    public WriteResultBefore(FrontendConnection con) {
        this.con = con;
    }

    @Override
    public WriteResultBO apply(TupleDesc td) {
        SelectResponse selectResponse = new SelectResponse(td.numFields());
        // 获取buffer
        ByteBuf buffer = null;
        if (con != null) {
            ChannelHandlerContext ctx = con.getCtx();
            buffer = ctx.alloc().buffer();
        }
        Integer count = 0;

        return new WriteResultBO(selectResponse, buffer, count);
    }




}
