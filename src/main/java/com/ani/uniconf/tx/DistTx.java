package com.ani.uniconf.tx;

import com.ani.uniconf.repository.ConfRepoConnector;
import com.ani.uniconf.repository.distlock.DistLock;
import com.ani.utils.core.AniByte;
import com.ani.utils.exception.AniDataException;
import com.ani.utils.exception.AniRuleException;

import java.util.concurrent.CountDownLatch;

/**
 * Created by yeh on 17-2-20.
 */
public class DistTx {

    public enum State {
        READY,
        PROCESSING,
        FINISHED
    }

    private String cluster = "default";

    private String topic = "default";

    private String txId = "default";

    private TxExecutor txExecutor;

    private TxEventListener txEventListener;

    private State state = State.READY;

    private ConfRepoConnector connector;

    private DistLock lock;

    public DistTx() {
    }

    public DistTx(String cluster, String topic, String txId, TxExecutor txExecutor, TxEventListener txEventListener, ConfRepoConnector connector) throws AniDataException, AniRuleException {
        super();
        this.cluster = cluster;
        this.topic = topic;
        this.txId = txId;
        this.txExecutor = txExecutor;
        this.txEventListener = txEventListener;
        this.connector = connector;
        initLock();
    }

    private void initLock() throws AniDataException, AniRuleException {
        this.lock = this.connector.obtainLock(
                this.getTxPath(),
                State.READY.ordinal());
        updateTxStateFromLock();
    }

    public void start() throws AniDataException, AniRuleException {
        try {
            lock.acquire();
            updateTxStateFromLock();
            if (state == State.FINISHED) {
                return;
            }
            setState(State.PROCESSING);
            executeTx();
            setState(State.FINISHED);
            lock.release();
        } catch (AniDataException e) {
            e.printStackTrace();
        } catch (AniRuleException e) {
            e.printStackTrace();
        }
    }

    private void updateTxStateFromLock() throws AniDataException, AniRuleException {
        int lockStateNum = this.lock.getState();
        if (lockStateNum > (State.values().length - 1)) {
            this.setState(State.READY);
        } else {
            this.state = getStateFromInt(lockStateNum);
        }
    }

    private State getStateFromInt(int stateNum) {
        return State.values()[stateNum];
    }

    private void executeTx() throws AniRuleException {
        if (!isLegal()) throw new AniRuleException(
                "TX_EXECUTOR_MUST_BE_INDICATED");
        this.txExecutor.execute(this);
        this.txExecutor.onFinished(this);
    }

    public boolean isLegal() {
        if (this.txExecutor == null) return false;
        return true;
    }

    public void setState(State state) throws AniDataException, AniRuleException {
        if ((state.ordinal() == this.state.ordinal()) ||
                (state == null)) return;
        this.lock.setState(state.ordinal());
        this.state = state;
        if (this.txEventListener != null)
            this.txEventListener.onStateChanged(this);
    }

    public State getState() throws AniDataException {
        this.state = this.getStateFromInt(this.lock.getState());
        return this.state;
    }

    public void setStateFromBytes(byte[] stateBytes) throws AniDataException, AniRuleException {
        int stateOrdinal;
        if (stateBytes == null) {
            stateOrdinal = State.READY.ordinal();
        } else {
            stateOrdinal = new AniByte(stateBytes).getIntValue();
        }
        setState(State.values()[stateOrdinal]);
    }

    public byte[] getStateByte() {
        return new AniByte(this.state.ordinal()).getBytes();
    }

    public void setTxEventListener(TxEventListener txEventListener) {
        this.txEventListener = txEventListener;
    }

    public void setTxExecutor(TxExecutor txExecutor) {
        this.txExecutor = txExecutor;
    }

    private String[] txPath;

    public String[] getTxPath() {
        if (txPath == null) {
            this.txPath = new String[]{
                    this.cluster,
                    "tx",
                    this.topic,
                    this.txId
            };
        }
        return this.txPath;
    }
}