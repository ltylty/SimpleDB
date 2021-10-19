package engine.net.handler.frontend;

import engine.net.proto.util.ErrorCode;

/**
 * SavePointHandler
 *
 * @Author lizhuyang
 */
public final class SavepointHandler {

    public static void handle(String stmt, FrontendConnection c) {
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
    }

}
