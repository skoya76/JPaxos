package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Pre-vote request sent before suspecting the leader.
 */
public class PreVoteRequest extends Message {
    private static final long serialVersionUID = 1L;

    private final long roundId;
    private final int candidateId;

    public PreVoteRequest(int view, long roundId, int candidateId) {
        super(view);
        this.roundId = roundId;
        this.candidateId = candidateId;
    }

    public PreVoteRequest(DataInputStream input) throws IOException {
        super(input);
        roundId = input.readLong();
        candidateId = input.readInt();
    }

    public PreVoteRequest(ByteBuffer bb) {
        super(bb);
        roundId = bb.getLong();
        candidateId = bb.getInt();
    }

    public long getRoundId() {
        return roundId;
    }

    public int getCandidateId() {
        return candidateId;
    }

    @Override
    public MessageType getType() {
        return MessageType.PreVoteRequest;
    }

    @Override
    public int byteSize() {
        return super.byteSize() + 8 + 4;
    }

    @Override
    protected void write(ByteBuffer bb) {
        bb.putLong(roundId);
        bb.putInt(candidateId);
    }
}
