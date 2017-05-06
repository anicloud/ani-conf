package com.ani.test.uniconf.core;

import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.ZkConnector;
import com.ani.uniconf.confnode.ConfNode;
import com.ani.uniconf.confnode.HostNode;
import com.ani.utils.exception.AniDataException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 17-4-11.
 */
public class TestHostNode {
    ConfRepoConnector zkCon;

    @Before
    public void connect() throws AniDataException {
        this.zkCon = new ZkConnector(
                "127.0.0.1", "2181"
        );
    }

    @Test
    public void testSingleNode() throws AniDataException, InterruptedException {
        initNode(1);
        Thread.sleep(100000l);
    }

    @Test
    public void testMultiNodesWatcher() throws AniDataException, InterruptedException {
//        List<AniConfNode> hostNodes = new ArrayList<AniConfNode>(10);
        final CountDownLatch latch = new CountDownLatch(1);
        final int[] nodeNum = {1};
        while(nodeNum[0] < 10){
            nodeNum[0]++;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        initNode(nodeNum[0]);
                        latch.await();
                    } catch (AniDataException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).run();
        }
        latch.countDown();

    }

    public void initNode(int num) throws AniDataException {
        ConfNode testHost = new HostNode(
                "bj-test",
                "slave",
                new byte[]{(byte)127, (byte)0, (byte)0, (byte)num},
                new byte[]{},
                zkCon
        );
    }
}
