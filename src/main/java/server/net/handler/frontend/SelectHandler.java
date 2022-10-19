package server.net.handler.frontend;

import server.net.response.SelectDatabase;
import server.net.response.SelectVersion;
import server.net.response.SelectVersionComment;
import server.net.response.jdbc.SelectIncrementResponse;
import server.parser.ServerParse;
import server.parser.ServerParseSelect;

/**
 * SelectHandler
 *
 * @Author lizhuyang
 */
public final class SelectHandler {

    private static String selectIncrement = "SELECT @@session.auto_increment_increment";

    public static void handle(String stmt, FrontendConnection c, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c);
                break;
            default:
                if (selectIncrement.equals(stmt)) {
                    SelectIncrementResponse.response(c);
                } else {
                    c.execute(stmt, ServerParse.SELECT);
                }
                break;
        }
    }

}