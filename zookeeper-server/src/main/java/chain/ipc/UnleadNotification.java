package chain.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class UnleadNotification extends ProtoNotification {

    public final static short NOTIFICATION_ID = 111;


    public UnleadNotification() {
        super(NOTIFICATION_ID);
    }
}
