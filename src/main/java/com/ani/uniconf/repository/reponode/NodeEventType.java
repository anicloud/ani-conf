package com.ani.uniconf.repository.reponode;

/**
 * Created by yeh on 16-12-23.
 */
public enum NodeEventType {
    NONE,
    ADDED,
    UPDATED,
    REMOVED,
    CHILD_ADDED,
    CHILD_UPDATED,
    CHILD_REMOVED,
    CONNECTION_SUSPENDED,
    CONNECTION_RECONNECTED,
    CONNECTION_LOST,
    INITIALIZED;
}
