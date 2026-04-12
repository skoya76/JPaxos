package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class AliveReplyTest extends AbstractMessageTestCase<AliveReply> {
    private int view = 9;
    private long heartbeatId = 1234;
    private AliveReply aliveReply;

    @Before
    public void setUp() {
        aliveReply = new AliveReply(view, heartbeatId);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(view, aliveReply.getView());
        assertEquals(heartbeatId, aliveReply.getHeartbeatId());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(aliveReply);
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.AliveReply, aliveReply.getType());
    }

    @Override
    protected void compare(AliveReply expected, AliveReply actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getHeartbeatId(), actual.getHeartbeatId());
    }
}
