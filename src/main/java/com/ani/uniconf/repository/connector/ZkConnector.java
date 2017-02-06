package com.ani.uniconf.repository.connector;

import com.ani.uniconf.core.AniConfNode;
import com.ani.uniconf.repository.event.NodeEventListener;
import com.ani.uniconf.repository.event.NodeEventType;
import com.ani.uniconf.repository.event.ZkNodeEvent;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.Map;

/**
 * Created by yeh on 16-12-17.
 */
public class ZkConnector extends ConfRepoConnector {

    private CuratorFramework client;

    private static String parentPath = null;

    private List<String> nodes;

    public ZkConnector() {
        super();
    }

    public ZkConnector(String confRepoAddress, String confRepoPort) {
        super(confRepoAddress, confRepoPort);
    }

    public ZkConnector(String confRepoAddress, String confRepoPort, int timeout) {
        super(confRepoAddress, confRepoPort, timeout);
    }

    @Override
    public synchronized void connect() throws AniDataException {
        try {
            this.client = CuratorFrameworkFactory.newClient(
                    String.format("%s:%s", this.confRepoAddress, this.confRepoPort),
                    this.timeout,
                    this.timeout * 2,
                    new ExponentialBackoffRetry(1000, 3)
            );
            this.client.start();
            this.initRootPaths();
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

    // node data
    private NodeCache parentNodeDataCache = null;

    private AniDataException generateNodeDataException(String msg, String path, Exception e) {
        return new AniDataException(
                String.join(" : ", msg, path, e.getMessage())
        );
    }

    private void initParentNodeDataCacheIfNeed() throws AniDataException {
        if (parentNodeDataCache != null)
            return;
        this.parentNodeDataCache = new NodeCache(this.client, this.getParentPath(), true);
        try {
            this.parentNodeDataCache.start(true);
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_PARENT_NODE_DATA_INIT_FAILED",
                    this.getParentPath(),
                    e
            );
        }
    }

    private byte[] getParentNodeData() throws AniDataException {
        return getNodeData(null);
    }

    @Override
    public void setNodeData(String[] childNodePath, byte[] nodeData) throws AniDataException {
        try {
            this.client.create().forPath(
                    this.getFullNodePath(childNodePath),
                    nodeData
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_PARENT_NODE_DATA_INIT_FAILED",
                    this.getFullNodePath(childNodePath),
                    e
            );
        }
    }


    public byte[] getNodeData(String[] childNodePath) throws AniDataException {
        try {
            return this.client.getData().forPath(
                    getFullNodePath(childNodePath)
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_PARENT_NODE_DATA_OBTAINING_FAILED",
                    this.getParentPath(),
                    e
            );
        }
    }

    @Override
    public byte[] watchParentNodeData(final boolean watch) throws AniDataException {
        if (watch) {
            this.initParentNodeDataCacheIfNeed();
            this.parentNodeDataCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    nodeData = parentNodeDataCache.getCurrentData().getData();
                    NodeEventType eventType = nodeData == null
                            ? NodeEventType.REMOVED
                            : NodeEventType.UPDATED;
                    nodeEventListener.processEvent(new ZkNodeEvent(
                            getParentPath(),
                            eventType,
                            nodeData
                    ));
                }
            });
        }
        return getParentNodeData();
    }

    private boolean isNodeExist(String path) throws AniDataException {
        try {
            if(this.client.checkExists().forPath(path) == null)
                return false;
            else
                return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_NODE_PATH_EXISTENCE_CHECK_FAILED: ",
                    path,
                    e
            );
        }
    }

    private void initRootPaths() throws AniDataException {
        try {
            if(isNodeExist(this.getParentPath()))
                return;
            this.client.create()
                    .creatingParentsIfNeeded()
                    .forPath(this.getParentPath());
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_PARENT_PATH_CREATING_FAILED: ",
                    this.getParentPath(),
                    e
            );
        }
    }

    private String getParentPath() {
        if (this.parentPath == null) {
            if (this.rootPath == null || this.rootPath.length < 1)
                return "/";
            StringBuilder curPath = new StringBuilder("");
            for (int i = 0; i < this.rootPath.length; i++) {
                curPath.append("/");
                curPath.append(this.rootPath[i]);
            }
            this.parentPath = curPath.toString();
        }
        return this.parentPath;
    }

    @Override
    public void setChildNode(String[] nodeChildPath, byte[] nodeData, AniConfNode.NodeType nodeType) throws AniDataException {
        String fullPath = getFullNodePath(nodeChildPath);
        try {
            if(isNodeExist(fullPath)) return;
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.valueOf(nodeType.name()))
                    .forPath(fullPath, this.nodeData);
        } catch (Exception e) {
            e.printStackTrace();
            throw generateNodeDataException(
                    "ANI_CONF_ZK_NODE_CREATING_ERROR",
                    fullPath,
                    e);
        }
    }

    @Override
    public List<String> getChildrenNodes(boolean watch) throws AniRuleException {
        final String curPath = this.getParentPath();
        PathChildrenCache childrenCache = new PathChildrenCache(
                this.client,
                curPath,
                true
        );
        try {
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            if (this.nodeEventListener != null && watch) {
                childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                        nodeEventListener.processEvent(new ZkNodeEvent(
                                event.getData().getPath(),
                                event.getType(),
                                event.getData().getData()
                        ));
                    }
                });
            }
            return this.client.getChildren().forPath(this.getParentPath());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AniRuleException("ANI_CONF_ZK_GET_CHILDREN_NODE_EXCEPTION : " + e.getMessage());
        }
    }

    private String getFullNodePath(String[] nodeChildPath) {
        if (nodeChildPath != null && nodeChildPath.length > 0) {
            String childNodePathStr = String.join("/", nodeChildPath);
            StringBuilder childNodeFullPath = new StringBuilder(
                    this.getParentPath().length()
                            + childNodePathStr.length() + 1
            );
            childNodeFullPath.append(this.getParentPath());
            if (!this.getParentPath().endsWith("/"))
                childNodeFullPath.append("/");
            childNodeFullPath.append(childNodePathStr);
            return childNodeFullPath.toString();
        } else {
            return this.getParentPath();
        }
    }
}