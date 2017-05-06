package com.ani.uniconf.repository.reponode;

import com.ani.utils.core.AniMapBuilder;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.Map;

/**
 * Created by yeh on 17-1-23.
 */
public class ZkNodeEvent extends NodeEvent {

    private static Map<Integer, NodeEventType> childrenNodeEventTypeMap = initChildrenTypeMap();

    private static Map<Integer, NodeEventType> initChildrenTypeMap(){
            return new AniMapBuilder<Integer, NodeEventType>(PathChildrenCacheEvent.Type.values().length)
                    .put(PathChildrenCacheEvent.Type.CHILD_ADDED.ordinal(), NodeEventType.CHILD_ADDED)
                    .put(PathChildrenCacheEvent.Type.CHILD_REMOVED.ordinal(), NodeEventType.CHILD_REMOVED)
                    .put(PathChildrenCacheEvent.Type.CHILD_UPDATED.ordinal(), NodeEventType.CHILD_UPDATED)
                    .put(PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED.ordinal(), NodeEventType.CONNECTION_SUSPENDED)
                    .put(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED.ordinal(), NodeEventType.CONNECTION_RECONNECTED)
                    .put(PathChildrenCacheEvent.Type.CONNECTION_LOST.ordinal(), NodeEventType.CONNECTION_LOST)
                    .getMap();
    }

    public ZkNodeEvent() {
        super();
    }

    public ZkNodeEvent(String path, NodeEventType nodeEventType, RepoNode node) {
        super(path, nodeEventType, node);
    }

    public ZkNodeEvent(String path, PathChildrenCacheEvent.Type childrenNodeEventType, RepoNode node) {
        super(path, childrenNodeEventTypeMap.get(childrenNodeEventType), node);
    }

    @Override
    public NodeEventType getType() {
        if(this.nodeEventType == null) return NodeEventType.NONE;
        return this.nodeEventType;
    }
}
