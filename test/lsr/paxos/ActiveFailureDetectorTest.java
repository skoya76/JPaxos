package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.BitSet;

import org.junit.Before;
import org.junit.Test;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.messages.AliveReply;
import lsr.paxos.messages.Message;
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
    public void shouldCalculateHeartbeatIntervalFromElectionTimeout() throws Exception {
        assertEquals(75, invokeComputeSuggestedHeartbeatInterval(150.0, 0.0, 0.999));
        assertEquals(51, invokeComputeSuggestedHeartbeatInterval(102.0, 0.0, 0.999));
    }

    @Test
    public void shouldRescheduleHeartbeatFromLastSendWhenIntervalShrinks() throws Exception {
        long now = ActiveFailureDetector.getTime();
        long lastSend = now - 200;

        setIntField(failureDetector, "view", 0);
        invokeMarkFollowerSent(failureDetector, 1, lastSend);
        invokeHandleAliveReply(failureDetector, new AliveReply(0, 1L, now - 100, 50L), 1);

        assertTrue(invokeScheduleDueFollowers(failureDetector, now).contains(Integer.valueOf(1)));
    }

    private static void setLastHeartbeatRcvdTS(ActiveFailureDetector detector, long timestamp)
            throws Exception {
        setLongField(detector, "lastHeartbeatRcvdTS", timestamp);
    }

    private static void setLongField(ActiveFailureDetector detector, String name, long value)
            throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(detector, value);
    }

    private static void setIntField(ActiveFailureDetector detector, String name, int value)
            throws Exception {
        Field field = ActiveFailureDetector.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setInt(detector, value);
    }

    private static int invokeComputeSuggestedHeartbeatInterval(double et, double packetLossRate,
                                                               double targetProbability)
            throws Exception {
        Method method = ActiveFailureDetector.class.getDeclaredMethod(
                "computeSuggestedHeartbeatInterval", double.class, double.class, double.class);
        method.setAccessible(true);
        return ((Integer) method.invoke(null, et, packetLossRate, targetProbability)).intValue();
    }

    private static void invokeMarkFollowerSent(ActiveFailureDetector detector,
                                               int followerId, long sendTs)
            throws Exception {
        Method method = ActiveFailureDetector.class.getDeclaredMethod(
                "markFollowerSentLocked", int.class, long.class);
        method.setAccessible(true);
        synchronized (detector) {
            method.invoke(detector, followerId, sendTs);
        }
    }

    private static void invokeHandleAliveReply(ActiveFailureDetector detector,
                                               AliveReply reply, int sender)
            throws Exception {
        Method method = ActiveFailureDetector.class.getDeclaredMethod(
                "handleAliveReply", AliveReply.class, int.class);
        method.setAccessible(true);
        method.invoke(detector, reply, sender);
    }

    @SuppressWarnings("unchecked")
    private static ArrayDeque<Integer> invokeScheduleDueFollowers(ActiveFailureDetector detector,
                                                                  long now)
            throws Exception {
        Method method = ActiveFailureDetector.class.getDeclaredMethod(
                "scheduleDueFollowersLocked", long.class);
        method.setAccessible(true);
        synchronized (detector) {
            return (ArrayDeque<Integer>) method.invoke(detector, now);
        }
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
