package chain.values;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Arrays;

public class AppOp extends PaxosValue {

    private final byte[] opData;

    public AppOp(byte[] opData) {
        super(Type.APP_OP);
        this.opData = opData;
    }

    public byte[] getOpData() {
        return opData;
    }

    @Override
    public String toString() {
        return "AppOp{" +
                "opData=" + opData.length +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppOp appOp = (AppOp) o;
        return Arrays.equals(opData, appOp.opData);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(opData);
    }

    static ValueSerializer serializer = new ValueSerializer<AppOp>() {
        @Override
        public void serialize(AppOp op, ByteBuf out) throws IOException {
            out.writeInt(op.opData.length);
            out.writeBytes(op.opData);
        }

        @Override
        public AppOp deserialize(ByteBuf in) throws IOException {
            int opDataSize = in.readInt();
            byte[] opData = new byte[opDataSize];
            in.readBytes(opData);
            return new AppOp(opData);
        }
    };
}
