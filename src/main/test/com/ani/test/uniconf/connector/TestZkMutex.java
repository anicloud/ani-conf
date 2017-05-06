package com.ani.test.uniconf.connector;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 17-4-11.
 */
public class TestZkMutex {

    private CuratorFramework client;

    @Before
    public void initCurator(){
        this.client = CuratorFrameworkFactory.newClient(
                String.format("%s:%s", "127.0.0.1", "2181"),
                1000,
                2000,
                new ExponentialBackoffRetry(1000, 3)
        );
        this.client.start();
    }

    @Test
    public void testCreateMutexNodeDuplicate(){
        try {
            this.client
                    .create()
                    .creatingParentContainersIfNeeded()
                    .forPath("/testLock", "test".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLock() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(10);
        for(int i = 0; i < 10; i++){
            final int finalI = i;
            new Thread(new Runnable() {
                public void run() {
                    acquireLock(finalI);
                    latch.countDown();
                }
            }).start();
            System.out.println(String.format("Thread %d created.", i));
        }
        latch.await();
    }
    private void acquireLock(int threadNum) {
        InterProcessMutex lock = new InterProcessMutex(this.client, "/testLock");
        try {
            lock.acquire();
            System.out.println(String.format("ACQUIRED: Thread %d", threadNum));
//            client.setData().forPath("/testLock", "acquired".getBytes());
            System.out.println(String.format("RELEASED: Thread %d", threadNum));
//            client.setData().forPath("/testLock", "released".getBytes());
            lock.release();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
