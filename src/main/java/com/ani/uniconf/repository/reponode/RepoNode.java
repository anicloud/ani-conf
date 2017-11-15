package com.ani.uniconf.repository.reponode;

import java.sql.Time;

/**
 * Created by yeh on 17-4-26.
 */
public class RepoNode {

    public byte[] data;

    public RepoNodeVersion version;

    public Time createTime;

    public RepoNode() {}

    public RepoNode(byte[] data, RepoNodeVersion version, Time createTime) {
        this.data = data;
        this.version = version;
        this.createTime = createTime;
    }
}
