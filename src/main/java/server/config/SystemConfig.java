package server.config;

/**
 * SystemConfig
 *
 * @Author lizhuyang
 */
public interface SystemConfig {



    String Database = "";
    // 36小时内连接不发起请求就干掉 秒为单位
    // long IDLE_TIME_OUT = 36 * 3600 * 1000;
    long IDLE_TIME_OUT = 36 * 3600;

    // 1小时做一次idle check 秒为单位
    //int IDLE_CHECK_INTERVAL = 3600 * 1000;
    int IDLE_CHECK_INTERVAL = 3600;

    String DEFAULT_CHARSET = "UTF-8";


}
