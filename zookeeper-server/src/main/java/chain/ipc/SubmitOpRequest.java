package chain.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class SubmitOpRequest extends ProtoRequest {

    public static final short REQUEST_ID = 125;

    private final byte[] op;

    public SubmitOpRequest(byte[] op) {
        super(REQUEST_ID);
        this.op = op;
    }

    public byte[] getOp() {
        return op;
    }

}
