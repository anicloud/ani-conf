package com.ani.uniconf.repository.event;

import com.ani.uniconf.repository.event.NodeEventType;

/**
 * Created by yeh on 16-12-23.
 */
public interface NodeEventListener {

    public void processEvent(NodeEvent event);

}
