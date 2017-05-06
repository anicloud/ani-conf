package com.ani.uniconf.repository.reponode;

/**
 * Created by yeh on 17-1-23.
 */
public abstract class NodeEvent {

    public abstract NodeEventType getType();

    public String path;

    public NodeEventType nodeEventType;

    public RepoNode node;

    public NodeEvent() {
    }

    public NodeEvent(String path, NodeEventType nodeEventType, RepoNode node) {
        this.path = path;
        this.nodeEventType = nodeEventType;
        this.node = node;
    }
}
