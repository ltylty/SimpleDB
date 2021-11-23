package server.net.response;

import server.net.handler.frontend.FrontendConnection;
import server.net.proto.mysql.ErrorPacket;

/**
 * ErrResponse
 *
 * @Author lizhuyang
 */
public class ErrResponse {

    public static void response(FrontendConnection connection, String errMsg) {
        if (errMsg != null && errMsg.length()>0) {
            ErrorPacket errorPacket = new ErrorPacket();
            errorPacket.message = errMsg.getBytes();
            errorPacket.write(connection.getCtx());
        }
    }
}
