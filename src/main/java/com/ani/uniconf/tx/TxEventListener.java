package com.ani.uniconf.tx;

/**
 * Created by yeh on 17-2-20.
 */
public interface TxEventListener {

    public void onStateChanged(DistTx tx);

}