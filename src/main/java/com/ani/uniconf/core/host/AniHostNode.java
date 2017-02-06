package com.ani.uniconf.core.host;

import com.ani.uniconf.core.AniConfNode;
import com.ani.uniconf.repository.event.NodeEventListener;
import com.ani.uniconf.repository.connector.ConfRepoConnector;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 16-12-17.
 */
public class AniHostNode extends AniConfNode {

    private String hostRole;

    private byte[] hostIp;


    public AniHostNode() {
        super();
    }

    public AniHostNode(
            String clusterName,
            String hostRole,
            byte[] hostIp,
            NodeEventListener eventListener,
            ConfRepoConnector connector) throws AniDataException {
        super(clusterName, ConfType.HOST, eventListener, connector);
        this.hostRole = hostRole;
        this.hostIp = hostIp;
        configNodeConnector();
    }

    public List<String> getNeighbours() throws AniRuleException {
        return this.connector.getChildrenNodes(true);
    }

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private void configNodeConnector() throws AniDataException {
        this.connector.setConfNode(this.getNodePath(), this.hostIp, this.eventListener);
        this.connector.connect();
        this.setHostNode();
    }

    /**
     * Get current host role node path
     * @return "/{clusterName}/{confType}/{hostRole}/{hostIp}"
     */
    protected String[] getNodePath() {
        return new String[] {
                this.clusterName,
                confType.name().toLowerCase(),
                this.hostRole
        };
    }

    protected void setHostNode() throws AniDataException {
        this.connector.setChildNode(new String[]{String.valueOf(this.hostIp)}, this.hostIp, NodeType.EPHEMERAL);
    }
}
