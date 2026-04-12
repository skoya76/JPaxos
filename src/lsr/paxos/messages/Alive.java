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

    /**
     * Creates new <code>Alive</code> message with specified view number and
     * highest instance ID + 1.
     */
    public Alive(int view, int logNextId) {
        this(view, logNextId, -1L);
    }

    public Alive(int view, int logNextId, long heartbeatId) {
        super(view);
        this.logNextId = logNextId;
        this.heartbeatId = heartbeatId;
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
    }

    public Alive(ByteBuffer bb) {
        super(bb);
        logNextId = bb.getInt();
        heartbeatId = bb.getLong();
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

    public MessageType getType() {
        return MessageType.Alive;
    }

    public int byteSize() {
        return super.byteSize() + 4 + 8;
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ", logsize: " + logNextId +
               ", heartbeatId: " + heartbeatId + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(logNextId);
        bb.putLong(heartbeatId);
    }
}
