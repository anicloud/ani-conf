package com.ani.uniconf.repository.distlock;

import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

/**
 * Created by yeh on 17-5-2.
 */
public abstract class DistLock {

    protected int stateNum;

    public boolean isAcquired;

    public abstract void acquire() throws AniDataException;

    public abstract void release() throws AniDataException;

    public abstract void setState(int stateNum) throws AniDataException, AniRuleException;

    public abstract int getState() throws AniDataException;

}
