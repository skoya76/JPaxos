package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.BitSet;

import org.junit.Before;
import org.junit.Test;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.PreVoteRequest;
import lsr.paxos.network.Network;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

public class ActiveFailureDetectorTest {
    private ActiveFailureDetector failureDetector;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        Storage storage = new InMemoryStorage();
        failureDetector = new ActiveFailureDetector(new FailureDetector.FailureDetectorListener() {
            @Override
            public void suspect(int view) {
            }
        }, new StubNetwork(), storage);
    }

    @Test
    public void shouldAllowRuntimeTimeoutOverridesAndRestoreDefaults() {
        assertEquals(1000, failureDetector.getDefaultSuspectTimeout());
        assertEquals(500, failureDetector.getDefaultSendTimeout());
        assertEquals(1000, failureDetector.getSuspectTimeout());
        assertEquals(500, failureDetector.getSendTimeout());

        failureDetector.setSuspectTimeout(250);
        failureDetector.setSendTimeout(125);

        assertEquals(250, failureDetector.getSuspectTimeout());
        assertEquals(125, failureDetector.getSendTimeout());
        assertEquals(1000, failureDetector.getDefaultSuspectTimeout());
        assertEquals(500, failureDetector.getDefaultSendTimeout());

        failureDetector.restoreDefaultTimeouts();

        assertEquals(1000, failureDetector.getSuspectTimeout());
        assertEquals(500, failureDetector.getSendTimeout());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveSuspectTimeout() {
        failureDetector.setSuspectTimeout(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveSendTimeout() {
        failureDetector.setSendTimeout(-1);
    }

    @Test
    public void shouldUseDefaultSuspectTimeoutForPreVoteGrants() throws Exception {
        ProcessDescriptorHelper.initialize(3, 1);
        ActiveFailureDetector follower = new ActiveFailureDetector(
                new FailureDetector.FailureDetectorListener() {
                    @Override
                    public void suspect(int view) {
                    }
                },
                new StubNetwork(),
                new InMemoryStorage());

        follower.setSuspectTimeout(1);
        setLastHeartbeatRcvdTS(follower, ActiveFailureDetector.getTime() - 10);

        synchronized (follower) {
            assertFalse(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 1L, 2)));
        }

        setLastHeartbeatRcvdTS(follower,
                ActiveFailureDetector.getTime() - follower.getDefaultSuspectTimeout() - 1);

        synchronized (follower) {
            assertTrue(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 2L, 2)));
        }
    }

    @Test
    public void shouldGrantPreVoteImmediatelyWhenLeaderIsUnknown() throws Exception {
        ProcessDescriptorHelper.initialize(3, 1);
        ActiveFailureDetector follower = new ActiveFailureDetector(
                new FailureDetector.FailureDetectorListener() {
                    @Override
                    public void suspect(int view) {
                    }
                },
                new StubNetwork(),
                new InMemoryStorage());

        setLastHeartbeatRcvdTS(follower, ActiveFailureDetector.getTime() - 10);

        synchronized (follower) {
            assertFalse(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 5L, 2)));
        }

        setLeaderKnown(follower, false);

        synchronized (follower) {
            assertTrue(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 6L, 2)));
        }
    }

    @Test
    public void shouldNotUsePreVoteBackoffAsHeartbeatEvidence() throws Exception {
        ProcessDescriptorHelper.initialize(3, 1);
        ActiveFailureDetector follower = new ActiveFailureDetector(
                new FailureDetector.FailureDetectorListener() {
                    @Override
                    public void suspect(int view) {
                    }
                },
                new StubNetwork(),
                new InMemoryStorage());

        long now = ActiveFailureDetector.getTime();
        setLastHeartbeatRcvdTS(follower, now - follower.getDefaultSuspectTimeout() - 1);
        setNextPreVoteNotBeforeTs(follower, now + 60_000);

        synchronized (follower) {
            assertTrue(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 3L, 2)));
        }
    }

    @Test
    public void shouldNotUseTimeoutRecalculationAsHeartbeatEvidence() throws Exception {
        ProcessDescriptorHelper.initialize(3, 1);
        ActiveFailureDetector follower = new ActiveFailureDetector(
                new FailureDetector.FailureDetectorListener() {
                    @Override
                    public void suspect(int view) {
                    }
                },
                new StubNetwork(),
                new InMemoryStorage());

        long oldHeartbeat = ActiveFailureDetector.getTime()
                - follower.getDefaultSuspectTimeout() - 1;
        setLastHeartbeatRcvdTS(follower, oldHeartbeat);

        follower.setSuspectTimeout(1);

        synchronized (follower) {
            assertTrue(follower.shouldGrantPreVoteLocked(new PreVoteRequest(0, 4L, 2)));
        }
        assertEquals(oldHeartbeat, getLastHeartbeatRcvdTS(follower));
    }

    private static void setLastHeartbeatRcvdTS(ActiveFailureDetector detector, long timestamp)
            throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField("lastHeartbeatRcvdTS");
        field.setAccessible(true);
        field.setLong(detector, timestamp);
    }

    private static long getLastHeartbeatRcvdTS(ActiveFailureDetector detector) throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField("lastHeartbeatRcvdTS");
        field.setAccessible(true);
        return field.getLong(detector);
    }

    private static void setNextPreVoteNotBeforeTs(ActiveFailureDetector detector, long timestamp)
            throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField("nextPreVoteNotBeforeTs");
        field.setAccessible(true);
        field.setLong(detector, timestamp);
    }

    private static void setLeaderKnown(ActiveFailureDetector detector, boolean leaderKnown)
            throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField("leaderKnown");
        field.setAccessible(true);
        field.setBoolean(detector, leaderKnown);
    }

    private static class StubNetwork extends Network {
        @Override
        protected void send(Message message, int destination) {
        }

        @Override
        protected void send(Message message, BitSet destinations) {
        }

        @Override
        public void start() {
        }
    }
}
