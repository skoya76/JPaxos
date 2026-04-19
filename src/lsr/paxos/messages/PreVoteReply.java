package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Response to {@link PreVoteRequest}.
 */
public class PreVoteReply extends Message {
    private static final long serialVersionUID = 1L;

    private final long roundId;
    private final boolean granted;

    public PreVoteReply(int view, long roundId, boolean granted) {
        super(view);
        this.roundId = roundId;
        this.granted = granted;
    }

    public PreVoteReply(DataInputStream input) throws IOException {
        super(input);
        roundId = input.readLong();
        granted = input.readBoolean();
    }

    public PreVoteReply(ByteBuffer bb) {
        super(bb);
        roundId = bb.getLong();
        granted = bb.get() != 0;
    }

    public long getRoundId() {
        return roundId;
    }

    public boolean isGranted() {
        return granted;
    }

    @Override
    public MessageType getType() {
        return MessageType.PreVoteReply;
    }

    @Override
    public int byteSize() {
        return super.byteSize() + 8 + 1;
    }

    @Override
    protected void write(ByteBuffer bb) {
        bb.putLong(roundId);
        bb.put((byte) (granted ? 1 : 0));
    }
}
