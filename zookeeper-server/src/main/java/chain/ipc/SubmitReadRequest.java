package chain.ipc;

import org.apache.zookeeper.server.Request;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class SubmitReadRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final Request request;

    public SubmitReadRequest(Request request) {
        super(REQUEST_ID);
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }
}
