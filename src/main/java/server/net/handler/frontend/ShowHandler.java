package server.net.handler.frontend;

import server.net.response.ErrResponse;
import server.net.response.ShowDatabases;
import server.net.response.ShowTables;
import server.parser.ServerParseShow;

/**
 * ShowHandler
 *
 * @Author lizhuyang
 */
public final class ShowHandler {

    public static void handle(String stmt, FrontendConnection c, int offset) {
        switch (ServerParseShow.parse(stmt, offset)) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;
            case ServerParseShow.SHOWTABLES:
                ShowTables.response(c);
                break;
            default:
                ErrResponse.response(c, "not support this set param");
                break;
        }
    }
}