package com.ani.uniconf.repository;

import com.ani.uniconf.repository.reponode.NodeCreateMode;
import com.ani.uniconf.repository.reponode.NodeEventListener;
import com.ani.uniconf.repository.distlock.DistLock;
import com.ani.uniconf.repository.reponode.RepoNode;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

import java.util.List;

/**
 * Created by yeh on 16-12-17.
 */
public abstract class ConfRepoConnector {

    protected String confRepoAddress = "127.0.0.1";

    protected String confRepoPort = "2181";

    protected int timeout = 1000;

    public ConfRepoConnector() {
    }

    public ConfRepoConnector(String confRepoAddress, String confRepoPort) {
        this.confRepoAddress = confRepoAddress;
        this.confRepoPort = confRepoPort;
    }

    public ConfRepoConnector(String confRepoAddress, String confRepoPort, int timeout) {
        this.confRepoAddress = confRepoAddress;
        this.confRepoPort = confRepoPort;
        this.timeout = timeout;
    }

    /**
     * Connect to physical repository server/cluster
     * @throws AniDataException
     */
    protected abstract void connect() throws AniDataException;

    /**
     * Disconnect from physical repository
     * @throws AniDataException
     */
    public abstract void disconnect() throws AniDataException;

    /**
     * Create node only
     * @return
     * @throws AniDataException
     */
    public abstract RepoNode createNode(String[] nodePath, byte[] nodeData, NodeCreateMode nodeCreateMode) throws AniDataException;

    /**
     * Set data to a configuration node
     * @param nodePath
     * @param nodeData
     * @param nodeCreateMode
     * @return
     * @throws AniDataException
     */
    public abstract RepoNode setNode(String[] nodePath, byte[] nodeData, NodeCreateMode nodeCreateMode) throws AniDataException;

    /**
     * Get data from a configuration node
     * @param nodePath
     * @param eventListener If null, never listen to the data of node.
     * @return
     * @throws AniDataException
     */
    public abstract RepoNode getNode(String[] nodePath, NodeEventListener eventListener) throws AniDataException;

    /**
     * Get children nodes and watch it(if need)
     * @param nodeEventListener
     * @return
     * @throws AniRuleException
     */
    public abstract List<String> getChildrenNodesPath(String[] parentNodePath, final NodeEventListener nodeEventListener) throws AniRuleException;

    /**
     * Obtain distributed lock
     * @param path
     * @param stateNum
     * @return
     */
    public abstract DistLock obtainLock(String[] path, int stateNum);

    protected abstract String getPathFromArray(String[] path);
}