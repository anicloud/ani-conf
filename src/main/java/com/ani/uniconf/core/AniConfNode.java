package com.ani.uniconf.core;

import com.ani.uniconf.repository.event.NodeEventListener;
import com.ani.uniconf.repository.connector.ConfRepoConnector;

/**
 * Created by yeh on 16-12-23.
 */
public abstract class AniConfNode {

    protected enum ConfType {
        CLUSTER,
        HOST,
        APP
    }

    public enum NodeType {
        PERSISTENT,
        PERSISTENT_SEQUENTIAL,
        EPHEMERAL,
        EPHEMERAL_SEQUENTIAL
    }

    protected String clusterName;

    protected ConfType confType;

    protected NodeEventListener eventListener;

    protected ConfRepoConnector connector;

    public AniConfNode() {
    }

    public AniConfNode(
            String clusterName,
            ConfType confType,
            NodeEventListener eventListener,
            ConfRepoConnector connector) {
        this.clusterName = clusterName;
        this.confType = confType;
        this.eventListener = eventListener;
        this.connector = connector;
    }

    /**
     * Get current node path
     * @return "/{clusterName}/{confType}"
     */
    protected abstract String[] getNodePath();

}
