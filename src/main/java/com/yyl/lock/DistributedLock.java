package com.yyl.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * author:yangyuanliang Date:2019-10-12 Time:14:08
 **/
@Slf4j
public class DistributedLock implements Lock, Watcher {
    private static final String ROOT_PATH = "/lock";//根节点

    private static final Long SESSION_TIMEOUT = 10000L;

    private static final String HOST_ADDRESS = "127.0.0.1:2181";

    private ZooKeeper zooKeeper;

    private String lockName;

    private CountDownLatch lockFlag = null;//判断其他线程是否释放了锁

    private String currentNodeName;//当前节点

    private String preNodeName;//前一个节点

    public DistributedLock(String lockName){
        this.lockName = lockName;
        try {
            zooKeeper = new ZooKeeper(HOST_ADDRESS, SESSION_TIMEOUT.intValue(), this);
            //如果zookeeper的连接断掉了(可能时zookeeper集群中有机器挂了)，继续重试，直到重新获得连接为止
            if(!zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)){
                while(true){
                    if(zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)){
                        break;
                    }
                }
            }
            Stat stat = zooKeeper.exists(ROOT_PATH, false);
            //如果不存在根节点，则创建一个
            //多线程并发时，因为都没有Root节点，可能会发生创建异常
            if (stat == null) {
                zooKeeper.create(ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
        }
        log.info("Init class DistributedLock over!");
    }


    public void lock() {
        try {
            if (this.tryLock()) {
                log.info("Success get the lock!");
                return;
            }
            waitForLock(preNodeName, SESSION_TIMEOUT);
        } catch (KeeperException e) {
            log.error("Execption happened when try to get the lock.e:", e);
        } catch (InterruptedException e) {
            log.error("Execption happened when try to get the lock.e:", e);
        }
    }

    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }
    /**
     *
     * @param preNode
     * @param waitTime
     * @return
     */
    private boolean waitForLock(String preNode, Long waitTime) throws KeeperException, InterruptedException {
        //获取前一个节点，同时注册监听
        Stat stat = zooKeeper.exists(ROOT_PATH + "/" + preNode, true);
        if (stat != null) {
            log.warn("Thread " + Thread.currentThread().getId() + " waiting for " + ROOT_PATH + "/" + preNode);
            //设置锁标识
            this.lockFlag = new CountDownLatch(1);
            //当前一个节点删除时，会调用CountDownLatch.countDown()方法，此时await被唤醒
            this.lockFlag.await(waitTime, TimeUnit.MILLISECONDS);
            this.lockFlag = null;
        }
        return true;
    }
    public boolean tryLock() {
        try {
            //1.创建一个临时节点
            currentNodeName = zooKeeper.create(ROOT_PATH + "/" + lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //2.获取当前根目录下的所有子节点0 = "myLock0000000005",1 = "myLock0000000006",2 = "myLock0000000007"
            List<String> allSubNodes = zooKeeper.getChildren(ROOT_PATH, false);
            Collections.sort(allSubNodes);
            String currentNode = currentNodeName.substring(currentNodeName.lastIndexOf("/") + 1);
            //3.判断当前临时节点是否是最小节点，如果是返回true
            if (currentNode.equals(allSubNodes.get(0))) {
                return true;
            }
            //如果当前节点不是最小节点，找到自己的前一个节点，返回false
            preNodeName = allSubNodes.get(Collections.binarySearch(allSubNodes, currentNode) - 1);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {

        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (this.tryLock()) {
            return true;
        }
        try {
            waitForLock(preNodeName, time);
        } catch (KeeperException e) {
        }
        return false;
    }

    public void unlock() {
        log.warn("Try to release lock!");
        try {
            zooKeeper.delete(currentNodeName, -1);
            currentNodeName = null;
            //关闭资源
            zooKeeper.close();
        } catch (InterruptedException e) {
            log.error("Execption happened when try to get the lock.e:", e);
        } catch (KeeperException e) {
            log.error("Execption happened when try to get the lock.e:", e);
        }
    }

    public Condition newCondition() {
        return null;
    }

    public void process(WatchedEvent watchedEvent) {
        if(this.lockFlag!=null){
            this.lockFlag.countDown();
        }
    }
}
