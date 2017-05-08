package com.ani.uniconf.confnode;

import com.ani.uniconf.repository.reponode.NodeCreateMode;
import com.ani.uniconf.repository.reponode.NodeEvent;
import com.ani.uniconf.repository.reponode.NodeEventListener;
import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.reponode.NodeEventType;
import com.ani.uniconf.tx.DistTx;
import com.ani.uniconf.tx.TxEventListener;
import com.ani.uniconf.tx.TxExecutor;
import com.ani.utils.core.AniByte;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Created by yeh on 16-12-17.
 */
public class HostNode extends ConfNode {

    private byte[] hostIp;

    public HostNode() {
        super();
    }

    public HostNode(
            String clusterName,
            String role,
            byte[] hostIp,
            byte[] data,
            ConfRepoConnector connector) throws AniDataException {
        super(clusterName, ConfType.HOST, role, data, connector);
        this.hostIp = hostIp;
    }

//    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    /**
     * Get current host role node path
     *
     * @return "/{clusterName}/{confType}/{hostRole}/{hostIp}:{data}"
     */
    protected String[] getNodePath() {
        return ArrayUtils.addAll(
                this.getNodeRootPath(),
                new String[]{
                        String.valueOf(this.hostIp)
                });
    }

    private String nodePathStr;

    protected String getNodePathStr() {
        if (this.nodePathStr == null) {
            StringBuilder nodePathStrBuilder = new StringBuilder("/");
            nodePathStrBuilder.append(
                    String.join("/", this.getNodePath())
            );
            this.nodePathStr = nodePathStrBuilder.toString();
        }
        return nodePathStr;
    }

    @Override
    public void create() throws AniDataException {
        this.connector.setNode(
                this.getNodePath(),
                this.data,
                NodeCreateMode.EPHEMERAL);
        listen();
    }

    @Override
    public void listen() throws AniDataException {
        final NodeEventListener curNodeListener = this.eventListener;
        this.connector.getNode(getNodePath(), new NodeEventListener() {
            public void processEvent(NodeEvent event) {
                data = event.node.data;
                if (curNodeListener != null) curNodeListener.processEvent(event);
            }
        });
    }

    private DistTx hostLostTx;

    public interface HostFailureListener {

        public void onHostHaltingFailed(byte[] hostIp);

        public void onFailureProcessingFinished(byte[] hostIp);

    }

    public void setHostNodeHaltingFailureHandlingTx(
            final String role,
            final HostFailureListener hostFailureListener)
            throws AniDataException, AniRuleException {
        String[] roleRootPath = getNodeRootPathByRole(role);
        this.connector.getChildrenNodesPath(roleRootPath, new NodeEventListener() {
            public void processEvent(NodeEvent event) {
                if (event.nodeEventType != NodeEventType.CHILD_REMOVED) {
                    return;
                }
                try {
                    final String hostIpByteStr = event.path;
                    final byte[] hostIpByte = new AniByte(hostIpByteStr).getBytes();
                    createNodeHalingFailureHandlingTx(
                            role,
                            hostIpByteStr,
                            new TxExecutor() {
                                public void execute(DistTx tx) {
                                    hostFailureListener.onHostHaltingFailed(
                                            hostIpByte
                                    );
                                }

                                public void onFinished(DistTx tx) {
                                    hostFailureListener.onFailureProcessingFinished(
                                            hostIpByte
                                    );
                                }
                            },
                            null);
                } catch (AniDataException e) {
                    e.printStackTrace();
                } catch (AniRuleException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void createNodeHalingFailureHandlingTx(
            String role, String hostIpByteStr, TxExecutor hostLostTxExecutor, TxEventListener eventListener
    ) throws AniDataException, AniRuleException {
        String topic = String.format("%s-host-halting-failure", role);
        hostLostTx = new DistTx(
                this.clusterName,
                topic,
                hostIpByteStr,
                hostLostTxExecutor,
                eventListener,
                this.connector);
        hostLostTx.start();
    }

    @Override
    public void terminate() throws AniDataException {
        //TODO
    }

}
