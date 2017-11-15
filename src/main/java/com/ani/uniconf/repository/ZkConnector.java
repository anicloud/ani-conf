package com.ani.uniconf.repository;

import com.ani.uniconf.repository.reponode.*;
import com.ani.uniconf.repository.distlock.DistLock;
import com.ani.uniconf.repository.distlock.ZkDistLock;
import com.ani.utils.core.AniByte;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.sql.Time;
import java.util.List;

/**
 * Created by yeh on 16-12-17.
 */
public class ZkConnector extends ConfRepoConnector {

    private CuratorFramework client;

    private List<String> nodes;

    public ZkConnector() {
        super();
    }

    public ZkConnector(String confRepoAddress, String confRepoPort) throws AniDataException {
        super(confRepoAddress, confRepoPort);
        connect();
    }

    public ZkConnector(String confRepoAddress, String confRepoPort, int timeout) throws AniDataException {
        super(confRepoAddress, confRepoPort, timeout);
        connect();
    }

    private AniDataException generateNodeDataException(String msg, String path, Exception e) {
        return new AniDataException(
                String.join(" : ", msg, path, e.getMessage())
        );
    }

    @Override
    protected synchronized void connect() throws AniDataException {
        try {
            this.client = CuratorFrameworkFactory.newClient(
                    String.format("%s:%s", this.confRepoAddress, this.confRepoPort),
                    this.timeout,
                    this.timeout * 2,
                    new ExponentialBackoffRetry(1000, 3)
            );
            this.client.start();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AniDataException(
                    "ANI_CONF_ZK_CONNECTION_ESTABLISHMENT_FAILED: "
                            + e.getMessage());
        }
    }

    @Override
    public synchronized void disconnect() throws AniDataException {
        try {
            this.client.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AniDataException(
                    "ANI_CONF_ZK_DISCONNECTION_FAILED: "
                            + e.getMessage());
        }
    }

    @Override
    public RepoNode createNode(String[] nodePath, byte[] nodeData, NodeCreateMode nodeCreateMode) throws AniDataException {
        String pathStr = this.getNodePathStr(nodePath);
        try {
            this.client.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(getNodeCreateMode(nodeCreateMode))
                    .forPath(pathStr, nodeData);
            return new RepoNode(nodeData, new RepoNodeVersion(),
                    new Time(this.client.checkExists().forPath(pathStr).getCtime()));
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException("NODE_CREATE_FAILED", pathStr, e);
        }

    }

    @Override
    public RepoNode setNode(String[] nodePath, byte[] nodeData, NodeCreateMode nodeCreateMode) throws AniDataException {
        RepoNode curNode = null;
        String pathStr = this.getNodePathStr(nodePath);
        try {
            this.client.create().creatingParentContainersIfNeeded()
                    .withMode(getNodeCreateMode(nodeCreateMode))
                    .forPath(
                            pathStr,
                            nodeData
                    );
            curNode = new RepoNode(nodeData, new RepoNodeVersion(), new Time(0l));
        } catch (Exception e) {
            Stat curState = null;
            try {
                curState = this.client.setData().forPath(pathStr, nodeData);
                curNode = getRepoNodeFromStat(nodeData, curState);
                return curNode;
            } catch (Exception e1) {
                e1.printStackTrace();
                throw generateNodeDataException(
                        "ANI_CONF_ZK_NODE_CREATING_FAILED",
                        this.getNodePathStr(nodePath), e);
            }
        }
        return curNode;
    }

    private CreateMode getNodeCreateMode(NodeCreateMode nodeCreateMode) {
        return CreateMode.valueOf(nodeCreateMode.name());
    }

    private RepoNode getRepoNodeFromStat(byte[] data, Stat nodeStat) {
        if (nodeStat == null) return new RepoNode(data, new RepoNodeVersion(), new Time(0l));
        return new RepoNode(
                data,
                new RepoNodeVersion(
                        nodeStat.getVersion(),
                        nodeStat.getCversion()
                ),
                new Time(nodeStat.getCtime()));
    }

    public RepoNode getNode(String[] nodePath) throws AniDataException {
        RepoNode node;
        try {
            String pathStr = getNodePathStr(nodePath);
            byte[] nodeData = this.client.getData().forPath(pathStr);
            Stat nodeStat = this.client.checkExists().forPath(pathStr);
            node = new RepoNode(nodeData, new RepoNodeVersion(
                    nodeStat.getVersion(),
                    nodeStat.getCversion()
            ),
                    new Time(nodeStat.getCtime()));
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_NODE_DATA_OBTAINING_FAILED",
                    this.getNodePathStr(nodePath), e);
        }
        return node;
    }

    @Override
    public RepoNode getNode(String[] nodePath, final NodeEventListener eventListener) throws AniDataException {
        if (eventListener != null) {
            final String nodePathStr = getNodePathStr(nodePath);
            final NodeCache nodeDataCache = new NodeCache(this.client, nodePathStr, false);
            try {
                nodeDataCache.start(true);
            } catch (Exception e) {
                e.printStackTrace();
                throw generateNodeDataException(
                        "ANI_CONF_ZK_NODE_DATA_CACHE_START_ERROR",
                        this.getNodePathStr(nodePath), e);
            }
            byte[] bytes = null;
            final AniByte nodeData = new AniByte(bytes);
            nodeDataCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    NodeEventType eventType = null;
                    if (nodeDataCache.getCurrentData() == null) {
                        eventType = NodeEventType.REMOVED;
                        nodeDataCache.close();
                    } else {
                        eventType = NodeEventType.UPDATED;
                        nodeData.setBytes(nodeDataCache.getCurrentData().getData());
                    }
                    eventListener.processEvent(new ZkNodeEvent(
                            nodePathStr,
                            eventType,
                            getRepoNodeFromStat(
                                    nodeData.getBytes(),
                                    nodeDataCache.getCurrentData().getStat())
                    ));
                }
            });
        }
        return getNode(nodePath);
    }

    private boolean isNodeExist(String[] path) throws AniDataException {
        String fullPath = this.getNodePathStr(path);
        try {
            if (this.client.checkExists().forPath(fullPath) == null)
                return false;
            else
                return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_NODE_PATH_EXISTENCE_CHECK_FAILED: ",
                    fullPath,
                    e
            );
        }
    }

    @Override
    public List<String> getChildrenNodesPath(String[] parentNodePath, final NodeEventListener nodeEventListener) throws AniRuleException {
        final String parentNodePathStr = getNodePathStr(parentNodePath);
        PathChildrenCache childrenCache = new PathChildrenCache(
                this.client,
                parentNodePathStr,
                true
        );
        try {
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            if (nodeEventListener != null) {
                childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                        nodeEventListener.processEvent(new ZkNodeEvent(
                                getPathTail(event.getData().getPath()),
                                event.getType(),
                                getRepoNodeFromStat(
                                        event.getData().getData(),
                                        event.getData().getStat())
                        ));
                    }
                });
            }
            return this.client.getChildren().forPath(getNodePathStr(parentNodePath));
        } catch (Exception e) {
            e.printStackTrace();
            throw new AniRuleException("ANI_CONF_ZK_GET_CHILDREN_NODE_EXCEPTION : " + e.getMessage());
        }
    }

    private String getPathTail(String fullPath) {
        if (fullPath == null) return "";
        String[] fullPathSegments = fullPath.split("/");
        if (fullPathSegments == null || fullPathSegments.length < 1) return fullPath;
        return fullPathSegments[fullPathSegments.length - 1];
    }

    @Override
    protected String getPathFromArray(String[] path) {
        return new StringBuilder("/")
                .append(String.join("/", path))
                .toString();
    }

    private String getNodePathStr(String[] nodePath) {
        if (nodePath == null || nodePath.length < 1) return "/";
        StringBuilder pathBuilder = new StringBuilder("/");
        pathBuilder.append(String.join("/", nodePath));
        return pathBuilder.toString();
    }

    // Lock
    @Override
    public DistLock obtainLock(String[] path, int stateNum) {
        return new ZkDistLock(this.client, path, stateNum);
    }
}