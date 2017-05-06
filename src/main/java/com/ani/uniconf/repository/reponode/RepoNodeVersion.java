package com.ani.uniconf.repository.reponode;

/**
 * Created by yeh on 17-4-26.
 */
public class RepoNodeVersion {

    int dataVersion = 0;

    int childrenVersion = 0;

    public RepoNodeVersion() {
        this.dataVersion = 0;
        this.childrenVersion = 0;
    }

    public RepoNodeVersion(int dataVersion, int childrenVersion) {
        this.dataVersion = dataVersion;
        this.childrenVersion = childrenVersion;
    }
}
