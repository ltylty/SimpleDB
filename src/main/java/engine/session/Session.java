package engine.session;

import engine.net.handler.frontend.FrontendConnection;
import simpledb.Parser;
import simpledb.Transaction;

import java.io.IOException;

/**
 * @Author lizhuyang
 */
public class Session {
    // session 对应的连接
    private FrontendConnection conn;
    // 是否自动提交
    private boolean isAutoCommit;
    // 当前session下的事务
    Transaction t;

    public Session(FrontendConnection conn) {
        this.conn = conn;
        t = new Transaction();
    }

    public void begin() {
        t.start();
    }

    public void commit() {
        try {
            t.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
        try {
            t.abort();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(String sql, FrontendConnection connection) {
        t.start();
        Parser p = new Parser();
        p.setTransaction(t);
        p.processNextStatement(sql+";");
    }
}
