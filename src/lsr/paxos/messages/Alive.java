package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents <code>Alive</code> message used by <code>FailureDetector</code> to
 * determine status of the current leader.
 */
public class Alive extends Message {
    private static final long serialVersionUID = 1L;
    /**
     * LogSize is the size of log (the highest started instance ID) of the
     * leader.
     */
    private final int logNextId;
    private final long heartbeatId;
    private long heartbeatTimestamp;
    private final long rtt;
    private final long heartbeatInterval;

    /**
     * Creates new <code>Alive</code> message with specified view number and
     * highest instance ID + 1.
     */
    public Alive(int view, int logNextId) {
        this(view, logNextId, -1L, -1L, -1L, -1L);
    }

    public Alive(int view, int logNextId, long heartbeatId) {
        this(view, logNextId, heartbeatId, -1L, -1L, -1L);
    }

    public Alive(int view, int logNextId, long heartbeatId, long rtt,
                 long heartbeatInterval) {
        this(view, logNextId, heartbeatId, -1L, rtt, heartbeatInterval);
    }

    public Alive(int view, int logNextId, long heartbeatId, long heartbeatTimestamp, long rtt,
                 long heartbeatInterval) {
        super(view);
        this.logNextId = logNextId;
        this.heartbeatId = heartbeatId;
        this.heartbeatTimestamp = heartbeatTimestamp;
        this.rtt = rtt;
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Creates new <code>Alive</code> message from input stream with serialized
     * message inside.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    public Alive(DataInputStream input) throws IOException {
        super(input);
        logNextId = input.readInt();
        heartbeatId = input.readLong();
        heartbeatTimestamp = input.readLong();
        rtt = input.readLong();
        heartbeatInterval = input.readLong();
    }

    public Alive(ByteBuffer bb) {
        super(bb);
        logNextId = bb.getInt();
        heartbeatId = bb.getLong();
        heartbeatTimestamp = bb.getLong();
        rtt = bb.getLong();
        heartbeatInterval = bb.getLong();
    }

    /**
     * Returns the log next id from sender of this message.
     */
    public int getLogNextId() {
        return logNextId;
    }

    public long getHeartbeatId() {
        return heartbeatId;
    }

    public long getHeartbeatTimestamp() {
        return heartbeatTimestamp;
    }

    public void setHeartbeatTimestamp(long heartbeatTimestamp) {
        this.heartbeatTimestamp = heartbeatTimestamp;
    }

    public long getRtt() {
        return rtt;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public MessageType getType() {
        return MessageType.Alive;
    }

    public int byteSize() {
        return super.byteSize() + 4 + 8 + 8 + 8 + 8;
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ", logsize: " + logNextId +
               ", heartbeatId: " + heartbeatId +
               ", heartbeatTimestamp: " + heartbeatTimestamp +
               ", rtt: " + rtt +
               ", heartbeatInterval: " + heartbeatInterval + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(logNextId);
        bb.putLong(heartbeatId);
        bb.putLong(heartbeatTimestamp);
        bb.putLong(rtt);
        bb.putLong(heartbeatInterval);
    }
}
