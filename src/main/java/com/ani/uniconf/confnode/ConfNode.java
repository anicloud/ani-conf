package com.ani.uniconf.confnode;

import com.ani.uniconf.repository.reponode.NodeEventListener;
import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.reponode.RepoNodeVersion;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

/**
 * Created by yeh on 16-12-23.
 */
public abstract class ConfNode {

    protected enum ConfType {
        CLUSTER,
        HOST,
        APP
    }

    protected String clusterName;

    protected ConfType confType;

    protected String role;

    protected byte[] data;

    protected RepoNodeVersion version;

    protected NodeEventListener eventListener;

    protected ConfRepoConnector connector;

    public ConfNode() {}

    public ConfNode(
            String clusterName,
            ConfType confType,
            String role,
            byte[] data,
            ConfRepoConnector connector) {
        this.clusterName = clusterName;
        this.confType = confType;
        this.role = role;
        this.data = data;
        this.connector = connector;
    }

    public void setConnector(ConfRepoConnector connector){
        this.connector = connector;
    }

    public ConfRepoConnector getConnector(){
        return this.connector;
    }

    /**
     * Get current node path
     * @return "/{clusterName}/{confType}"
     */
    protected abstract String[] getNodePath();

    protected String[] getNodeRootPath(){
        return new String[]{
                this.clusterName,
                this.confType.name().toLowerCase(),
                this.role
        };
    }

    /**
     * Create configuration node.
     * @throws AniDataException
     */
    public abstract void create() throws AniDataException;

    /**
     * Set event before listen
     * @param eventListener
     */
    public void setEventListener(NodeEventListener eventListener){
        this.eventListener = eventListener;
    }

    /**
     * Listen to node data or children node changes
     * @throws AniRuleException
     * @throws AniDataException
     */
    public abstract void listen() throws AniDataException;

    /**
     * Terminate current node listener and jobs
     * @throws AniDataException
     */
    public abstract void terminate() throws AniDataException;

}
