package server;

import engine.TableStats;

/**
 * @Author lizhuyang
 */
public class DatabaseInstance {

    private static DatabaseInstance databaseInstance;
    // 默认端口号
    private int serverPort = 8090;
    // 默认用户名密码
    private String userName = "pay";
    private String passWd = "123";


    // 单例模式
    static {
        databaseInstance = new DatabaseInstance();
        // 加载数据
        String catalogFile = "dblp_data/dblp_simpledb.schema";
        engine.Database.getCatalog().loadSchema(catalogFile);
        TableStats.computeStatistics();
    }

    public static DatabaseInstance getInstance() {
        return databaseInstance;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWd() {
        return passWd;
    }

    public void setPassWd(String passWd) {
        this.passWd = passWd;
    }

}
