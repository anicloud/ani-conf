package com.ani.uniconf.repository.event;

/**
 * Created by yeh on 17-1-23.
 */
public abstract class NodeEvent {

    public abstract NodeEventType getType();

    protected String path;

    protected NodeEventType nodeEventType;

    protected byte[] data;

    public NodeEvent() {
    }

    public NodeEvent(String path, NodeEventType nodeEventType, byte[] data) {
        this.path = path;
        this.nodeEventType = nodeEventType;
        this.data = data;
    }
}
