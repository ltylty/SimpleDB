package server.net.handler.frontend;

import server.net.proto.mysql.OkPacket;
import server.net.proto.util.ErrorCode;
import server.net.proto.util.Isolations;
import server.net.response.CharacterSet;
import server.parser.ServerParse;
import server.parser.ServerParseSet;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SetHandler
 *
 * @Author lizhuyang
 */
public final class SetHandler {

    private static final Logger logger = LoggerFactory.getLogger(SetHandler.class);
    private static final byte[] AC_OFF = new byte[] {7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};
    private static final String TABLE_PATH_SYNTAX_ERROR = "must set like this [table_path=\"tableName:filePath\"";

    public static void handle(String stmt, FrontendConnection c, int offset) {
        ChannelHandlerContext ctx = c.getCtx();
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
            case ServerParseSet.AUTOCOMMIT_ON:
                if (c.isAutocommit()) {
                    c.writeBuf(OkPacket.OK);
                } else {
                    //c.commit();
                    c.setAutocommit(true);
                }
                break;
            case ServerParseSet.AUTOCOMMIT_OFF: {
                if (c.isAutocommit()) {
                    c.setAutocommit(false);
                }
                c.writeOk();
                break;
            }
            case ServerParseSet.TX_READ_UNCOMMITTED: {
                c.setTxIsolation(Isolations.READ_UNCOMMITTED);
                c.writeOk();
                break;
            }
            case ServerParseSet.TX_READ_COMMITTED: {
                c.setTxIsolation(Isolations.READ_COMMITTED);
                c.writeOk();
                break;
            }
            case ServerParseSet.TX_REPEATED_READ: {
                c.setTxIsolation(Isolations.REPEATED_READ);
                c.writeOk();
                break;
            }
            case ServerParseSet.TX_SERIALIZABLE: {
                c.setTxIsolation(Isolations.SERIALIZABLE);
                c.writeOk();
                break;
            }
            case ServerParseSet.NAMES:
                String charset = stmt.substring(rs >>> 8).trim();
                if (c.setCharset(charset)) {
                    c.writeOk();
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                }
                break;
            case ServerParseSet.CHARACTER_SET_CLIENT:
            case ServerParseSet.CHARACTER_SET_CONNECTION:
            case ServerParseSet.CHARACTER_SET_RESULTS:
                CharacterSet.response(stmt, c, rs);
                break;
            default:
                StringBuilder s = new StringBuilder();
                //logger.warn(s.append(c).append(stmt).append(" is not executed").toString());
                c.execute(stmt, ServerParse.SET);
        }
    }

}