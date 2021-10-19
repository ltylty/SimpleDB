package engine.net.handler.frontend;

import engine.net.response.SelectDatabase;
import engine.net.response.SelectVersion;
import engine.net.response.SelectVersionComment;
import engine.net.response.jdbc.SelectIncrementResponse;
import engine.parser.ServerParse;
import engine.parser.ServerParseSelect;

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
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c);
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