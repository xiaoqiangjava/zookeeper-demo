package com.xq.lean.zook;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 使用Zookeeper API操作Zookeeper集群
 * @author xiaoqiang
 * @date 2019/7/23 0:07
 */
public class ZookDemo
{
    private static final Logger logger = LoggerFactory.getLogger(ZookDemo.class);

    private ZooKeeper zkClient;

    private final String connect = "learn:2181";

    private final int sessionTimeout = 2000;

    /**
     * 初始化Zookeeper客户端
     * @throws IOException IOException
     */
    @Before
    public void init() throws IOException
    {
        // 创建客户端
        zkClient = new ZooKeeper(connect, sessionTimeout, (event) ->
        {
            // 当监听到事件发生变化时，发送通知
            logger.info("监听到的事件类型：" + event.getType().name());
            logger.info("监听事件发生的节点：" + event.getPath());
            // 当监听到事件后继续监听
            try
            {
                List<String> children = zkClient.getChildren("/apiTest", true);
                logger.info("发生变化后的列表：" + children);
            }
            catch (KeeperException e)
            {
                logger.error(e.getMessage(), e);
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
            }
        });
    }

    /**
     * 创建永久节点
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void createPersistent() throws KeeperException, InterruptedException
    {
        zkClient.create("/apiTest/child3/child31", "api create child".getBytes(Charset.forName("UTF-8")),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 创建临时节点: 客户端断开之后自动删除，所以执行完成之后立马删除
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void createEphemeral() throws KeeperException, InterruptedException
    {
        zkClient.create("/apiTest/ephemeral", "ephemeral node".getBytes(Charset.forName("UTF-8")),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * 节点是否存在
     */
    @Test
    public void testExist() throws KeeperException, InterruptedException
    {
        Stat stat = zkClient.exists("/apiTest/child4", false);
        logger.info("/apiTest是否存在：" + (stat == null ? false : true));
    }

    /**
     * 列出子节点，需要监听时指定watch为true或者创建一个Watch实例
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testList() throws KeeperException, InterruptedException
    {
        // 启用监听，当/apiTest节点下面的子节点发生变化时调用监听函数, 只会监听下一次子节点变化
        List<String> children = zkClient.getChildren("/apiTest", true);
        logger.info("/apiTest's children: " + children);
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 更新节点数据内容
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testUpdateData() throws KeeperException, InterruptedException
    {
        // version为-1时表示所有的版本内容都要修改
        Stat stat = zkClient.setData("/apiTest", "update data".getBytes(Charset.forName("UTF-8")), -1);
        logger.info("更新节点内容：" + (null == stat ? false : true));
    }

    /**
     * 获取节点内容: 当启用监听时，如果调用了set方法更新节点内容，就会收到监听事件，不管内容是否发生变化
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testGetData() throws KeeperException, InterruptedException
    {
        byte[] data = zkClient.getData("/apiTest", true, null);
        logger.info("节点内容：" + new String(data, Charset.forName("UTF-8")));
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 删除指定的节点: 只能删除非空目录
     * @throws KeeperException KeeperException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testDelete() throws KeeperException, InterruptedException
    {
        zkClient.delete("/apiTest/child3", -1);
    }
}
