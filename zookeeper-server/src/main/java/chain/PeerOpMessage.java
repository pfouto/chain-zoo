package chain;

import io.netty.buffer.ByteBuf;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.Request;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class PeerOpMessage extends ProtoMessage {

    public static final short MSG_CODE = 113;

    private final Request request;

    public PeerOpMessage(Request request) {
        super(MSG_CODE);
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }

    @Override
    public String toString() {
        return "PeerOpMessage{" +
                "request=" + request +
                '}';
    }

    public static final ISerializer<PeerOpMessage> serializer = new ISerializer<PeerOpMessage>() {

        @Override
        public void serialize(PeerOpMessage msg, ByteBuf out) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream oa = new DataOutputStream(baos);
            oa.writeLong(msg.request.sessionId);
            oa.writeInt(msg.request.cxid);
            oa.writeInt(msg.request.type);
            if (msg.request.request != null) {
                msg.request.request.rewind();
                int len = msg.request.request.remaining();
                byte[] b = new byte[len];
                msg.request.request.get(b);
                msg.request.request.rewind();
                oa.write(b);
            }
            oa.close();
            byte[] toWrite = baos.toByteArray();

            out.writeInt(toWrite.length);
            out.writeBytes(toWrite);
        }

        @Override
        public PeerOpMessage deserialize(ByteBuf in) throws IOException {
            byte[] toRead = new byte[in.readInt()];
            in.readBytes(toRead);

            ByteBuffer bb = ByteBuffer.wrap(toRead);
            long sessionId = bb.getLong();
            int cxid = bb.getInt();
            int type = bb.getInt();
            bb = bb.slice();
            Request si;
            if (type == ZooDefs.OpCode.sync) {
                throw new IllegalStateException(); //Not supporting syncs
            } else {
                si = new Request(null, sessionId, cxid, type, bb, Collections.emptyList());
            }

            return new PeerOpMessage(si);
        }
    };
}
