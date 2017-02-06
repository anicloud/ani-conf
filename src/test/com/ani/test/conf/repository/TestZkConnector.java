package com.ani.test.conf.repository;

import com.ani.uniconf.core.host.AniHostNode;
import com.ani.uniconf.repository.event.NodeEvent;
import com.ani.uniconf.repository.event.NodeEventType;
import com.ani.uniconf.repository.event.NodeEventListener;
import com.ani.uniconf.repository.connector.ZkConnector;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 17-1-1.
 */
public class TestZkConnector {

    CountDownLatch connectedSemaphore = new CountDownLatch(1);

    @Test
    public void testConnect() throws AniDataException {

        class TestEventListener implements NodeEventListener {
            public void processEvent(NodeEvent event) {
                System.out.println(String.format("Event received: %s", event.toString()));
            }
        }
        new Thread(new Runnable() {
            public void run() {
                AniHostNode testHostNode = null;
                try {
                    testHostNode = new AniHostNode(
                            "bj-yatsen-test",
                            "bus",
                            new byte[]{(byte) 127, (byte) 0, (byte) 0, (byte) 1},
                            new TestEventListener(),
                            new ZkConnector("127.0.0.1", "2181")
                    );
                } catch (AniDataException e) {
                    e.printStackTrace();
                }
                System.out.println("Current Children: ");
                try {
                    for (String oneChild : testHostNode.getNeighbours()) {
                        System.out.println(oneChild);
                    }
                } catch (AniRuleException e) {
                    e.printStackTrace();
                }
                System.out.println("== All children printed ==");

            }
        }).start();

        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new AniDataException("HOST_DISCONNECTED");
        }
    }


}
