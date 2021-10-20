package engine.net.handler.frontend;

import engine.net.proto.util.ErrorCode;
import engine.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.Parser;
import simpledb.Transaction;

/**
 * ServerQueryHandler
 *
 * @Author liutianyuan
 */
public class ServerQueryHandler implements FrontendQueryHandler {

    private static final Logger logger = LoggerFactory.getLogger("sql-digest");

    private FrontendConnection source;

    public ServerQueryHandler(FrontendConnection source) {
        this.source = source;
    }




    @Override
    public void query(String origin) {

        logger.info("sql = " + origin);
        String sql = removeFirstAnnotation(origin);
        int rs = ServerParse.parse(sql);
        switch (rs & 0xff) {
            case ServerParse.SET:
                SetHandler.handle(sql, source, rs >>> 8);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, source, rs >>> 8);
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, source, rs >>> 8);
                break;
            case ServerParse.START:
                StartHandler.handle(sql, source, rs >>> 8);
                break;
            case ServerParse.SAVEPOINT:
                SavepointHandler.handle(sql, source);
                break;
            case ServerParse.KILL:
                KillHandler.handle(sql, rs >>> 8, source);
                break;
            case ServerParse.KILL_QUERY:
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                break;
            case ServerParse.EXPLAIN:
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                break;
            case ServerParse.CREATE_DATABASE:
                // source.createShema(sql);
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, source, rs >>> 8);
                break;
            default:
                // todo add no modify exception
                source.execute(sql, rs);
        }
    }

    public static String removeFirstAnnotation(String sql) {
        String result = null;
        sql = sql.trim();
        if (sql.startsWith("/*")) {
            int index = sql.indexOf("*/") + 2;
            return sql.substring(index);
        } else {
            return sql;
        }
    }

}
