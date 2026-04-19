package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class PreVoteRequestTest extends AbstractMessageTestCase<PreVoteRequest> {
    private PreVoteRequest request;

    @Before
    public void setUp() {
        request = new PreVoteRequest(7, 13L, 2);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(7, request.getView());
        assertEquals(13L, request.getRoundId());
        assertEquals(2, request.getCandidateId());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(request);
    }

    @Override
    protected void compare(PreVoteRequest expected, PreVoteRequest actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getRoundId(), actual.getRoundId());
        assertEquals(expected.getCandidateId(), actual.getCandidateId());
    }
}
