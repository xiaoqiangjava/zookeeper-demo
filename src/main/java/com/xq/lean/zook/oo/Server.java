package com.xq.lean.zook.oo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 服务器动态上下线案例
 * 服务端启动的时候在zk集群中创建一个临时节点，将自身的hostName写入到节点中，客户端去监听节点的变化，做出相应的操作
 * @author xiaoqiang
 * @date 2019/7/23 1:45
 */
public class Server
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    static final String PARENT_PATH = "/servers";

    static final String CONNECT = "learn:2181";

    static final int SESSION_TIMEOUT = 2000;

    private static ZooKeeper zkClient;

    private static void init()
    {
        try
        {
            logger.info("init zkClient...");
            zkClient = new ZooKeeper(CONNECT, SESSION_TIMEOUT, (event) ->
            {
                // 记录事件发生的路径以及事件类型
                logger.info(event.getPath() + "--->" + event.getType().name());
            });
            // 创建父目录
            Stat stat = zkClient.exists(PARENT_PATH, false);
            if (null == stat)
            {
                logger.info("Created to parent path for servers.");
                zkClient.create(PARENT_PATH, PARENT_PATH.getBytes(Charset.forName("UTF-8")),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Succeed to created parent path.");
            }
            else
            {
                logger.info("Parent path already exist.");
            }
            logger.info("End init.");
        }
        catch (IOException | KeeperException | InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 服务端启动之后向Zookeeper集群注册，即创建临时节点
     * @param hostName 注册的服务端地址
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    private static void register2Zookeeper(String hostName)
    {
        try
        {
            logger.info("Start register: " + hostName);
            // 在父节点下面创建创建临时节点，节点数据为本身的hostName
            zkClient.create(PARENT_PATH + "/server", hostName.getBytes(Charset.forName("UTF-8")),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info("Succeed to register: " + hostName);
        }
        catch (KeeperException | InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args)
    {
        init();
        register2Zookeeper(args[0]);
        try
        {
            // 业务逻辑
            logger.info(args[0] + ": is working....");
            Thread.sleep(Integer.MAX_VALUE);
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
    }
}
