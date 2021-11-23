package server.session;

import server.net.handler.frontend.FrontendConnection;

/**
 * @Author lizhuyang
 */
public class SessionFactory {

    public static Session newSession(FrontendConnection connection) {
        Session session = new Session(connection);
        return session;
    }
}
