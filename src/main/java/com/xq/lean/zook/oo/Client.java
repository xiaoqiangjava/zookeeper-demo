package com.xq.lean.zook.oo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 客户端监听服务端创建子目录的父目录，当节点发生变化时，刷新客户端可以连接的服务器列表
 * @author xiaoqiang
 * @date 2019/7/23 2:11
 */
public class Client
{
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static ZooKeeper zkClient;

    /**
     * 获取zkclient
     */
    private static void init()
    {
        try
        {
            zkClient = new ZooKeeper(Server.CONNECT, Server.SESSION_TIMEOUT, (event) ->
            {
                // 记录事件发生的路径以及事件类型
                logger.info(event.getPath() + "--->" + event.getType().name());
                // 继续监听父目录的变化
                List<String> servers = getServers();
                logger.info("servers: " + servers);
            });
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);
        }


    }

    /**
     * 获取服务器列表信息
     * @return servers
     */
    public static List<String> getServers()
    {
        logger.info("Get server list...");
        // 存储服务器列表
        List<String> servers = new ArrayList<>();
        try
        {
            List<String> childList = zkClient.getChildren(Server.PARENT_PATH, true);
            for (String child : childList)
            {
                byte[] dataByte = zkClient.getData(Server.PARENT_PATH + "/" + child, false, null);
                servers.add(new String(dataByte, Charset.forName("UTF-8")));
            }
            logger.info("Succeed to get server list.");
        }
        catch (KeeperException | InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
        return servers;
    }

    public static void main(String[] args) throws InterruptedException
    {
        init();
        List<String> servers = getServers();
        // 业务逻辑
        logger.info("获取到的服务器列表：" + servers);

        Thread.sleep(Integer.MAX_VALUE);
    }
}
