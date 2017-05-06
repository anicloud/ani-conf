package com.ani.test.uniconf.connector;

import com.ani.uniconf.Uniconf;
import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.ZkConnector;
import com.ani.uniconf.tx.DistTx;
import com.ani.uniconf.tx.TxEventListener;
import com.ani.uniconf.tx.TxExecutor;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 17-4-11.
 */
public class TestTx {

    private Uniconf uniconf;

    private String topic;

    private String txId;

    Random threadIdRandom = new Random(1000);

    @Before
    public void initUniconf() throws AniDataException {
        ConfRepoConnector connector = new ZkConnector(
                "127.0.0.1", "2181", 1000
        );
        this.uniconf = new Uniconf(connector, "test-cluster");
        this.topic = "test-topic";
        this.txId = "test-txid";
    }

    private TxExecutor getTxExecutor(final int threadId) {
        return new TxExecutor() {
            int id = threadId;
            public void execute(DistTx tx) {
                try {
                    String msg = String.format(
                            "[%d THREAD][EXECUTOR] executing tx, state is %s"
                            ,this.id
                            ,tx.getState().toString()
                    ).toString();
                    System.out.println(msg);
                } catch (AniDataException e) {
                    e.printStackTrace();
                }
            }

            public void onFinished(DistTx tx) {
                try {
                    String msg = String.format(
                            "[%d THREAD][EXECUTOR FINISHED] finished, state is %s"
                            ,this.id
                            ,tx.getState().toString()
                    );
                    System.out.println(msg);
                } catch (AniDataException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private TxEventListener getTxListener(final int threadId) {
        return new TxEventListener() {
            int id = threadId;
            public void onStateChanged(DistTx tx) {
                try {
                    String msg = String.format(
                            "[%d THREAD][ON STATE CHANGED] finished, state is %s"
                            ,this.id
                            ,tx.getState().toString()
                    );
                    System.out.println(msg);
                } catch (AniDataException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    @Test
    public void testTxCreatingSingleThread() throws AniDataException, AniRuleException {
        DistTx tx = this.uniconf.getDistTx(
                this.topic,
                this.txId,
                getTxExecutor(1),
                getTxListener(1)
        );
        tx.start();
    }

    @Test
    public void testTxCreatingMultiThread() throws AniDataException, AniRuleException, InterruptedException {
        int threadNum = 10;
        final CountDownLatch latch = new CountDownLatch(threadNum);
        for(int i = 0; i < threadNum; i++){
            final int finalI = i;
            new Thread(new Runnable() {
                public void run() {
                    DistTx tx = null;
                    try {
                        tx = uniconf.getDistTx(
                                topic,
                                txId,
                                getTxExecutor(finalI),
                                getTxListener(finalI)
                        );
                        tx.start();
                    } catch (AniDataException e) {
                        e.printStackTrace();
                    } catch (AniRuleException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                    System.out.println(String.format("Thread %d finished", finalI));
                }
            }).start();
            System.out.println(String.format("Thread %d created", finalI));
        }
        latch.await();
    }
}
