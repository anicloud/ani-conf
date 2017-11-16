package com.ani.test.uniconf.core;

import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.ZkConnector;
import com.ani.uniconf.confnode.ConfNode;
import com.ani.uniconf.confnode.HostNode;
import com.ani.utils.core.AniByte;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
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
        initNode("slave", 1);
        Thread.sleep(100000l);
    }

    @Test
    public void testClusterMasterInit() throws AniDataException, AniRuleException {
        initMasterNode(1);
    }

    @Test
    public void testMultiNodesWatcher() throws AniDataException, InterruptedException {
//        List<AniConfNode> hostNodes = new ArrayList<AniConfNode>(10);
        final CountDownLatch latch = new CountDownLatch(1);
        final int[] nodeNum = {1};
        while (nodeNum[0] < 10) {
            nodeNum[0]++;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        initNode("slave", nodeNum[0]);
                        latch.await();
                    } catch (AniDataException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        latch.countDown();

    }

    @Test
    public void testHaltingFailureHandler() throws InterruptedException {
        int masterNum = 10;
        int slaveNum = 10;
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < masterNum; i++) {
            final int finalI = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        initMasterNode(finalI);
                        latch.await();
                    } catch (AniDataException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (AniRuleException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        for (int j = 0; j < slaveNum; j++) {
            final int finalJ = j;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        initNode("slave", finalJ);
                    } catch (AniDataException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        latch.await();
    }

    private void initMasterNode(int num) throws AniDataException, AniRuleException {
        initNode("master", num)
                .setHostNodeHaltingFailureHandlingTx(
                        "slave",
                        new HostNode.HostFailureListener() {
                            public void onHostHaltingFailed(String hostIp) {
                                String msg = String.format(
                                        "Host halted: %s, by master %d",
                                        hostIp.toString(),
                                        num).toString();
                                System.out.println(msg);
                            }

                            public void onFailureProcessingFinished(String hostIp) {
                                System.out.println(hostIp.toString());
                            }
                        });
    }

    public HostNode initNode(String role, int num) throws AniDataException {
        HostNode curNode = new HostNode(
                "bj-test",
                role,
                "127.0.0." + num,
                new byte[]{},
                zkCon
        );
        curNode.create();
        return curNode;
    }
}
