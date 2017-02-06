package com.ani.uniconf.repository.connector;

import com.ani.uniconf.core.AniConfNode;
import com.ani.uniconf.repository.event.NodeEventListener;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

import java.util.List;

/**
 * Created by yeh on 16-12-17.
 */
public abstract class ConfRepoConnector {

    protected String confRepoAddress = "127.0.0.1";

    protected String confRepoPort = "2181";

    protected int timeout = 10000;

    protected String[] rootPath = new String[]{"ani", "default"};

    protected byte[] nodeData = "".getBytes();

    protected NodeEventListener nodeEventListener;

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

    public void setConfNode(String[] rootPath, byte[] nodeData, NodeEventListener nodeEventListener) {
        this.rootPath = rootPath;
        this.nodeData = nodeData == null ? "".getBytes() : nodeData;
        this.nodeEventListener = nodeEventListener;
    }

    public abstract void connect() throws AniDataException;

    public abstract void disconnect() throws AniDataException;

    public abstract void setNodeData(String[] childNodePath, byte[] nodeData) throws AniDataException;

    public abstract byte[] getNodeData(String[] childNodePath) throws AniDataException;

    public abstract byte[] watchParentNodeData(boolean watch) throws AniDataException;

    public abstract void setChildNode(String[] nodeChildPath, byte[] nodeData, AniConfNode.NodeType nodeType) throws AniDataException;

    public abstract List<String> getChildrenNodes(boolean watch) throws AniRuleException;
}