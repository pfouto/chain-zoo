package chain.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class ExecuteOpNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 135;

    private final byte[] op;
    private final long instId;

    public ExecuteOpNotification(byte[] op) {
        this(op, -1);
    }

    public ExecuteOpNotification(byte[] op, long instId) {
        super(NOTIFICATION_ID);
        this.instId = instId;
        this.op = op;
    }

    public byte[] getOp() {
        return op;
    }

    public long getInstId() {
        return instId;
    }
}
