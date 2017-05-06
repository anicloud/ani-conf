package com.ani.test.uniconf;

import com.ani.uniconf.Uniconf;
import com.ani.uniconf.repository.ZkConnector;
import com.ani.uniconf.repository.reponode.NodeEvent;
import com.ani.uniconf.repository.reponode.NodeEventListener;
import com.ani.utils.exception.AniDataException;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by yeh on 17-4-21.
 */
public class TestUniconf {
    Uniconf uniconf;

    @Before
    public void connect() throws AniDataException {
        this.uniconf = new Uniconf(
                new ZkConnector("127.0.0.1", "2181"),
                "bj-test"
        );
    }

    @Test
    public void testHostInit() throws InterruptedException {
        try {
            uniconf.initHost("slave", new byte[]{(byte) 127, (byte) 0, (byte) 0, (byte) 1},
                    new byte[]{},
                    new NodeEventListener() {
                        public void processEvent(NodeEvent event) {
                            System.out.println(String.format(
                                    "Event: \n%s\n%s\n%s",
                                    event.path,
                                    new String(event.node.data),
                                    event.getType().toString()
                            ).toString());
                        }
                    });
        } catch (AniDataException e) {
            e.printStackTrace();
        }
        Thread.sleep(100000l);
    }
}
