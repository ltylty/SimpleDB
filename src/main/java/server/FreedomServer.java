package server;

import server.config.SocketConfig;
import server.net.handler.factory.FrontHandlerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 无毁的湖光 启动器
 *
 * @Author lizhuyang
 */
public class FreedomServer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(FreedomServer.class);
    public static final int BOSS_THREAD_COUNT = 1;

    public static void main(String[] args) {
        FreedomServer server = new FreedomServer();
        try {
            server.start();
            while (true) {
                try {
                    Thread.sleep(1000 * 300);
                }catch (Exception e){
                    // just ignore it
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        logger.info("Start Freedom");
        startServer();
    }

    public void startServer() {
        // acceptor , one port => one thread
        EventLoopGroup bossGroup = new NioEventLoopGroup(BOSS_THREAD_COUNT);
        // worker
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // Freedom Server
            DatabaseInstance databaseInstance = DatabaseInstance.getInstance();
            ServerBootstrap b = new ServerBootstrap();
            // 这边的childHandler是用来管理accept的
            // 由于线程间传递的是byte[],所以内存池okay
            // 只需要保证分配ByteBuf和write在同一个线程(函数)就行了
            b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childHandler(new FrontHandlerFactory())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, SocketConfig.CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.SO_TIMEOUT, SocketConfig.SO_TIMEOUT);
            ChannelFuture f = b.bind(databaseInstance.getServerPort()).sync();
            f.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            logger.error("监听失败" + e);
        }
    }

}
