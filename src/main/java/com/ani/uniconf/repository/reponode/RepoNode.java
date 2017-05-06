package com.ani.uniconf.repository.reponode;

/**
 * Created by yeh on 17-4-26.
 */
public class RepoNode {

    public byte[] data;

    public RepoNodeVersion version;

    public RepoNode() {}

    public RepoNode(byte[] data, RepoNodeVersion version) {
        this.data = data;
        this.version = version;
    }
}
