package engine.sql;

import engine.net.handler.frontend.FrontendConnection;
import engine.net.proto.util.Fields;
import engine.net.response.SelectResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.util.function.BiFunction;
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
