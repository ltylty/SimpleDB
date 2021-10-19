package engine.net.handler.frontend;

import engine.net.proto.util.ErrorCode;
import engine.parser.ServerParse;
import engine.parser.ServerParseStart;

/**
 * StartHandler
 *
 * @Author lizhuyang
 */
public final class StartHandler {

    public static void handle(String stmt, FrontendConnection c, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
                break;
            default:
                // todo data source
                  c.execute(stmt, ServerParse.START);
                break;
        }
    }

}