package com.ani.uniconf.tx;

/**
 * Created by yeh on 17-2-20.
 */
public interface TxExecutor {

    public void execute(DistTx tx);

    public void onFinished(DistTx tx);

}