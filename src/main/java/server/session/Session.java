package server.session;

import server.net.handler.frontend.FrontendConnection;
import server.sql.Parser;

/**
 * @Author lizhuyang
 */
public class Session {
    // session 对应的连接
    private FrontendConnection conn;

    Parser p = new Parser();

    public Session(FrontendConnection conn) {
        this.conn = conn;
    }

    public void execute(String sql, FrontendConnection connection) {
        p.processNextStatement(sql+";", connection);
    }
}
