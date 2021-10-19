package engine.sql;

import engine.net.handler.frontend.FrontendConnection;
import engine.net.response.OkResponse;

import java.util.function.Consumer;

/**
 * @Author: 刘天元
 * @Date: 2021/10/19 16:53
 */

public class WriteResultAfter implements Consumer<WriteResultBO> {

    FrontendConnection con;

    public WriteResultAfter(FrontendConnection con) {
        this.con = con;
    }

    @Override
    public void accept(WriteResultBO writeResultBO) {
        if(writeResultBO.getCount() == 0) {
            OkResponse.response(con);
        } else {
            writeResultBO.getSelectResponse().writeLastEof(con, writeResultBO.getBuffer());
        }

    }

}
