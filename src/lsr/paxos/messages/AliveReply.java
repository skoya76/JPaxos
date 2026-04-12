package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reply sent by follower after receiving an {@link Alive} heartbeat.
 */
public class AliveReply extends Message {
    private static final long serialVersionUID = 1L;

    private final long heartbeatId;

    public AliveReply(int view, long heartbeatId) {
        super(view);
        this.heartbeatId = heartbeatId;
    }

    public AliveReply(DataInputStream input) throws IOException {
        super(input);
        heartbeatId = input.readLong();
    }

    public AliveReply(ByteBuffer bb) {
        super(bb);
        heartbeatId = bb.getLong();
    }

    public long getHeartbeatId() {
        return heartbeatId;
    }

    @Override
    public MessageType getType() {
        return MessageType.AliveReply;
    }

    @Override
    public int byteSize() {
        return super.byteSize() + 8;
    }

    @Override
    protected void write(ByteBuffer bb) {
        bb.putLong(heartbeatId);
    }

    @Override
    public String toString() {
        return "ALIVE_REPLY (" + super.toString() + ", heartbeatId: " + heartbeatId + ")";
    }
}
