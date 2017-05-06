package com.ani.uniconf;

import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.confnode.HostNode;
import com.ani.uniconf.repository.reponode.NodeEventListener;
import com.ani.uniconf.tx.DistTx;
import com.ani.uniconf.tx.TxEventListener;
import com.ani.uniconf.tx.TxExecutor;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

/**
 * Created by yeh on 17-3-18.
 */
public class Uniconf {

    private ConfRepoConnector connector;

    private String clusterName;

    public Uniconf() {
    }

    public Uniconf(ConfRepoConnector connector, String clusterName) {
        this.connector = connector;
        this.clusterName = clusterName;
    }

    public HostNode initHost(
            String role,
            byte[] hostIp,
            byte[] data,
            NodeEventListener hostEventListener
    ) throws AniDataException {
        HostNode curHost = new HostNode(
                this.clusterName,
                role,
                hostIp,
                data,
                this.connector
        );
        if (hostEventListener != null) {
            curHost.setEventListener(hostEventListener);
        }
        curHost.create();
        return curHost;
    }

    public void listenHost(HostNode hostNode) throws AniDataException {
        if (hostNode.getConnector() == null)
            hostNode.setConnector(this.connector);
        hostNode.listen();
    }

    public DistTx getDistTx(
            String topic,
            String txId,
            TxExecutor txExecutor,
            TxEventListener txEventListener
    ) throws AniDataException, AniRuleException {
        DistTx tx = new DistTx(
                this.clusterName,
                topic,
                txId,
                txExecutor,
                txEventListener,
                this.connector
        );
        return tx;
    }
}
