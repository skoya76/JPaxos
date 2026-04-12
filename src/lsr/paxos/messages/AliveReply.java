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
    private final long heartbeatInterval;

    public AliveReply(int view, long heartbeatId) {
        this(view, heartbeatId, -1L);
    }

    public AliveReply(int view, long heartbeatId, long heartbeatInterval) {
        super(view);
        this.heartbeatId = heartbeatId;
        this.heartbeatInterval = heartbeatInterval;
    }

    public AliveReply(DataInputStream input) throws IOException {
        super(input);
        heartbeatId = input.readLong();
        heartbeatInterval = input.readLong();
    }

    public AliveReply(ByteBuffer bb) {
        super(bb);
        heartbeatId = bb.getLong();
        heartbeatInterval = bb.getLong();
    }

    public long getHeartbeatId() {
        return heartbeatId;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    @Override
    public MessageType getType() {
        return MessageType.AliveReply;
    }

    @Override
    public int byteSize() {
        return super.byteSize() + 8 + 8;
    }

    @Override
    protected void write(ByteBuffer bb) {
        bb.putLong(heartbeatId);
        bb.putLong(heartbeatInterval);
    }

    @Override
    public String toString() {
        return "ALIVE_REPLY (" + super.toString() + ", heartbeatId: " + heartbeatId +
               ", heartbeatInterval: " + heartbeatInterval + ")";
    }
}
