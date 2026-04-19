package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class PreVoteReplyTest extends AbstractMessageTestCase<PreVoteReply> {
    private PreVoteReply reply;

    @Before
    public void setUp() {
        reply = new PreVoteReply(7, 13L, true);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(7, reply.getView());
        assertEquals(13L, reply.getRoundId());
        assertTrue(reply.isGranted());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(reply);
    }

    @Override
    protected void compare(PreVoteReply expected, PreVoteReply actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getRoundId(), actual.getRoundId());
        assertEquals(expected.isGranted(), actual.isGranted());
    }
}
