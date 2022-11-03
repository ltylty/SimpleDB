package server.session;

import server.net.handler.frontend.FrontendConnection;
import server.sql.SqlParser;

/**
 * @Author lizhuyang
 */
public class Session {
    // session 对应的连接
    private FrontendConnection conn;

    SqlParser p = new SqlParser();

    public Session(FrontendConnection conn) {
        this.conn = conn;
    }

    public void execute(String sql, FrontendConnection connection) {
        p.processNextStatement(sql+";", connection);
    }
}
