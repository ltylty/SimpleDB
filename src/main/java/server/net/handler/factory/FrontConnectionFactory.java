package server.net.handler.factory;


import server.config.SystemConfig;
import server.net.handler.frontend.FrontendConnection;
import server.net.handler.frontend.ServerQueryHandler;
import server.session.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * FrontendConnection 工厂类
 *
 * @Author lizhuyang
 */
public class FrontConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(FrontConnectionFactory.class);

    /**
     * MySql ThreadId Generator
     */
    private static final AtomicInteger ACCEPT_SEQ = new AtomicInteger(0);

    public FrontendConnection getConnection() {
        FrontendConnection connection = new FrontendConnection();
        connection.setQueryHandler(new ServerQueryHandler(connection));
        connection.setId(ACCEPT_SEQ.getAndIncrement());
        logger.info("connection Id=" + connection.getId());
        connection.setCharset(SystemConfig.DEFAULT_CHARSET);
        connection.setLastActiveTime();
        connection.setSession(SessionFactory.newSession(connection));
        return connection;
    }
}
