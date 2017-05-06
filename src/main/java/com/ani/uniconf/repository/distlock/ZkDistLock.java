package com.ani.uniconf.repository.distlock;

import com.ani.utils.core.AniByte;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * Created by yeh on 17-5-2.
 */
public class ZkDistLock extends DistLock {

    private CuratorFramework client;

    private InterProcessMutex lock;

    private String[] txPath;

    public ZkDistLock(){
        super();
    }

    public ZkDistLock(CuratorFramework client, String[] txPath, int stateNum) {
        this.txPath = txPath;
        this.client = client;
        this.stateNum = stateNum;
        this.isAcquired = false;
        initLock();
    }

    private String txPathStr = null;

    private String getTxPathStr() {
        if (txPathStr == null) {
            this.txPathStr = new StringBuilder("/")
                    .append(StringUtils.join(txPath, "/"))
                    .toString();
        }
        return txPathStr;
    }

    private void initLock() {
        try {
            this.client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(getTxPathStr(), new AniByte(0).getBytes());
        } catch (Exception e) {}
        this.lock = new InterProcessMutex(this.client, this.getTxPathStr());
    }

    @Override
    public void acquire() throws AniDataException {
        try {
            this.lock.acquire();
            this.isAcquired = true;
            initMutexNode();
            this.getState();
        } catch (Exception e) {
            e.printStackTrace();
            throw getLockException("ACQUIREMENT", e);
        }
    }

    @Override
    public void release() throws AniDataException {
        try {
            this.lock.release();
            this.isAcquired = false;
        } catch (Exception e) {
            e.printStackTrace();
            throw getLockException("RELEASING", e);
        }
    }

    private void initMutexNode() throws AniDataException {
        try {
            this.client
                    .create()
                    .creatingParentContainersIfNeeded()
                    .forPath(this.getTxPathStr(), new AniByte(this.stateNum).getBytes());
        } catch (Exception e) {
        }
    }

    @Override
    public void setState(int stateNum) throws AniDataException, AniRuleException {
        this.checkLockAcquired();
        try {
            client.setData().forPath(this.getTxPathStr(), new AniByte(stateNum).getBytes());
            this.stateNum = stateNum;
        } catch (Exception e) {
            e.printStackTrace();
            throw getLockException("STATE_UPDATING", e);
        }
    }

    @Override
    public int getState() throws AniDataException {
        try {
            byte[] state = this.client.getData().forPath(getTxPathStr());
            if (state == null) {
                setState(0);
                return getState();
            }
            this.stateNum = new AniByte(state).getIntValue();
            return this.stateNum;
        } catch (Exception e) {
            e.printStackTrace();
            throw getLockException("STATE_OBTAINING", e);
        }
    }

    private void checkLockAcquired() throws AniRuleException {
        if (!this.isAcquired) throw new AniRuleException("LOCK_NOT_ACQUIRED");
    }

    private AniDataException getLockException(String error, Exception e) {
        return new AniDataException(
                String.format("LOCK_%s_EXCEPTION: %s"
                        , error
                        , e.getMessage()));
    }
}
