package chain.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class LeadNotification extends ProtoNotification {

    public final static short NOTIFICATION_ID = 110;

    private final int seqN;

    public LeadNotification(int seqN) {
        super(NOTIFICATION_ID);
        this.seqN = seqN;
    }

    public int getSeqN() {
        return seqN;
    }
}
