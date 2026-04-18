package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.AliveReply;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents failure detector thread. If the current process is the leader,
 * then this class is responsible for sending <code>ALIVE</code> message every
 * amount of time. Otherwise is responsible for suspecting the leader. If there
 * is no message received from leader, then the leader is suspected to crash,
 * and <code>Paxos</code> is notified about this event.
 */
final public class ActiveFailureDetector implements Runnable, FailureDetector {

    /** How long to wait until suspecting the leader. In milliseconds */
    private volatile int suspectTimeout;
    /** How long the leader waits until sending heartbeats. In milliseconds */
    private volatile int sendTimeout;
    private final int defaultSuspectTimeout;
    private final int defaultSendTimeout;

    private final Network network;
    private final MessageHandler innerListener;
    private final Storage storage;
    private final Thread thread;

    // Written under synchronized(this); read from Network thread without lock
    // → volatile required for cross-thread visibility.
    private volatile int view;

    /** Follower role: reception time of the last heartbeat from the leader */
    private volatile long lastHeartbeatRcvdTS;
    /** Leader role: time when the last message or heartbeat was sent to all */
    private volatile long lastHeartbeatSentTS;
    /** Leader role: monotonically increasing heartbeat id per follower */
    private final Map<Integer, Long> nextHeartbeatIdByFollower = new HashMap<Integer, Long>();
    /** Leader role: last RTT observed from each follower */
    private final Map<Integer, Long> lastRttByFollower = new HashMap<Integer, Long>();
    /** Leader role: per-follower heartbeat interval overrides (in milliseconds). */
    private final Map<Integer, Integer> perFollowerSendTimeouts = new HashMap<Integer, Integer>();
    /** Leader role: next scheduled heartbeat send time per follower. */
    private final Map<Integer, Long> perFollowerNextSendTs = new HashMap<Integer, Long>();
    /** Follower role: observed RTT samples from leader heartbeats. */
    private final ArrayDeque<Long> observedRtts = new ArrayDeque<Long>();
    /** Follower role: observed heartbeat ids for loss estimation. */
    private final NavigableSet<Long> observedHeartbeatIds = new TreeSet<Long>();
    /** Follower role: latest computed E_t (suspicion timeout) in milliseconds. */
    private long lastComputedEt = -1;
    /** Follower role: latest suggested heartbeat interval for leader. */
    private int lastSuggestedHeartbeatInterval = -1;
    /** Follower role: true once tuning moves from warmup to active computation. */
    private boolean followerTuningStarted = false;

    private final FailureDetectorListener fdListener;

    /**
     * Initializes new instance of <code>FailureDetector</code>.
     * 
     * @param paxos - the paxos which should be notified about suspecting leader
     * @param network - used to send and receive messages
     * @param storage - storage containing all data about paxos
     */
    public ActiveFailureDetector(FailureDetectorListener fdListener, Network network,
                                 Storage storage) {
        this.fdListener = fdListener;
        this.network = network;
        this.storage = storage;
        defaultSuspectTimeout = processDescriptor.fdSuspectTimeout;
        defaultSendTimeout = processDescriptor.fdSendTimeout;
        suspectTimeout = defaultSuspectTimeout;
        sendTimeout = defaultSendTimeout;
        thread = new Thread(this, "FailureDetector");
        thread.setDaemon(true);
        innerListener = new InnerMessageHandler();
        storage.addViewChangeListener(viewChangeListener);
    }

    public int getDefaultSuspectTimeout() {
        return defaultSuspectTimeout;
    }

    public int getDefaultSendTimeout() {
        return defaultSendTimeout;
    }

    public int getSuspectTimeout() {
        return suspectTimeout;
    }

    public int getSendTimeout() {
        return sendTimeout;
    }

    int getPerFollowerSendTimeout(int replicaId) {
        synchronized (this) {
            Integer timeout = perFollowerSendTimeouts.get(replicaId);
            return timeout == null ? -1 : timeout.intValue();
        }
    }

    public void setSuspectTimeout(int suspectTimeout) {
        validateTimeout("suspectTimeout", suspectTimeout);
        synchronized (this) {
            this.suspectTimeout = suspectTimeout;
            lastHeartbeatRcvdTS = getTime();
            notifyAll();
        }
    }

    public void setSendTimeout(int sendTimeout) {
        validateTimeout("sendTimeout", sendTimeout);
        synchronized (this) {
            this.sendTimeout = sendTimeout;
            rescheduleDefaultFollowersLocked(getTime());
            notifyAll();
        }
    }

    public void restoreDefaultTimeouts() {
        synchronized (this) {
            suspectTimeout = defaultSuspectTimeout;
            sendTimeout = defaultSendTimeout;
            rescheduleDefaultFollowersLocked(getTime());
            notifyAll();
        }
    }

    public long getLastRttForReplica(int replicaId) {
        synchronized (this) {
            Long rtt = lastRttByFollower.get(replicaId);
            return rtt == null ? -1 : rtt.longValue();
        }
    }

    public int getObservedRttCount() {
        synchronized (this) {
            return observedRtts.size();
        }
    }

    public int getObservedHeartbeatIdCount() {
        synchronized (this) {
            return observedHeartbeatIds.size();
        }
    }

    public long getLastObservedRtt() {
        synchronized (this) {
            Long last = observedRtts.peekLast();
            return last == null ? -1 : last.longValue();
        }
    }

    public long getLastComputedEt() {
        synchronized (this) {
            return lastComputedEt;
        }
    }

    public int getLastSuggestedHeartbeatInterval() {
        synchronized (this) {
            return lastSuggestedHeartbeatInterval;
        }
    }

    /**
     * Starts failure detector.
     */
    public void start(int initialView) {
        synchronized (this) {
            view = initialView;
            thread.start();
        }
        // Any message received from the leader serves also as an ALIVE message.
        Network.addMessageListener(MessageType.ANY, innerListener);
        // Sent messages used when in leader role: also count as ALIVE message
        // so don't reset sending timeout.
        Network.addMessageListener(MessageType.SENT, innerListener);
    }

    /**
     * Stops failure detector.
     */
    public void stop() {
        Network.removeMessageListener(MessageType.ANY, innerListener);
        Network.removeMessageListener(MessageType.SENT, innerListener);
    }

    /**
     * Updates state of failure detector, due to leader change.
     * 
     * Called whenever the leader changes.
     * 
     * @param newLeader - process id of the new leader
     */
    protected Storage.ViewChangeListener viewChangeListener = new Storage.ViewChangeListener() {

        public void viewChanged(int newView, int newLeader) {
            synchronized (ActiveFailureDetector.this) {
                logger.debug("FD has been informed about view {}", newView);
                view = newView;
                lastHeartbeatRcvdTS = getTime();
                suspectTimeout = defaultSuspectTimeout;
                resetFollowerObservations();
                resetLeaderObservations();
                ActiveFailureDetector.this.notifyAll();
            }
        }
    };

    public void run() {
        logger.info("Starting failure detector");
        try {
            while (true) {
                long now = getTime();
                boolean localProcessLeader;
                int logNextId = -1;
                int viewSnapshot = -1;
                ArrayDeque<Integer> dueFollowers = null;
                synchronized (this) {
                    viewSnapshot = view;
                    localProcessLeader = processDescriptor.isLocalProcessLeader(viewSnapshot);
                    if (localProcessLeader) {
                        logNextId = storage.getLog().getNextId();
                        dueFollowers = scheduleDueFollowersLocked(getTime());
                    }
                }

                if (localProcessLeader) {
                    for (Integer replicaIdObject : dueFollowers) {
                        int replicaId = replicaIdObject.intValue();
                        if (view != viewSnapshot) {
                            logger.debug("View changed during per-follower send loop " +
                                    "(snapshot={}, current={}); aborting heartbeat burst.",
                                    viewSnapshot, view);
                            break;
                        }
                        Alive alive;
                        synchronized (this) {
                            long heartbeatId = nextHeartbeatIdForFollowerLocked(replicaId);
                            alive = createAliveForFollowerLocked(replicaId, logNextId, heartbeatId,
                                    viewSnapshot);
                        }
                        long sendTs = getTime();
                        alive.setHeartbeatTimestamp(sendTs);
                        network.sendMessage(alive, replicaId);
                        synchronized (this) {
                            markFollowerSentLocked(replicaId, sendTs);
                        }
                    }
                    lastHeartbeatSentTS = getTime();
                    now = lastHeartbeatSentTS;

                    synchronized (this) {
                        long nextSend = getNextLeaderSendTimeLocked(lastHeartbeatSentTS);
                        while (now < nextSend && processDescriptor.isLocalProcessLeader(view)) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Sending next Alive in {} ms", nextSend - now);
                            }
                            wait(nextSend - now);
                            now = getTime();
                            nextSend = getNextLeaderSendTimeLocked(lastHeartbeatSentTS);
                        }
                    }
                } else {
                    synchronized (this) {
                        lastHeartbeatRcvdTS = now;
                        long suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                        while (now < suspectTime && !processDescriptor.isLocalProcessLeader(view)) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Suspecting leader ({}) in {} ms",
                                        processDescriptor.getLeaderOfView(view), suspectTime - now);
                            }

                            wait(suspectTime - now);
                            now = getTime();
                            suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                        }
                        if (!processDescriptor.isLocalProcessLeader(view)) {
                            fdListener.suspect(view);
                            int oldView = view;
                            while (oldView == view) {
                                logger.debug("FD is waiting for view change from {}", oldView);
                                wait();
                            }
                            logger.debug("FD now knows about new view");
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Intersects any message sent or received, used to reset the timeouts for
     * sending and receiving ALIVE messages.
     * 
     * These methods are called by the Network thread.
     * 
     * @author Nuno Santos (LSR)
     */
    final class InnerMessageHandler implements MessageHandler {

        public void onMessageReceived(Message message, int sender) {
            if (processDescriptor.isLocalProcessLeader(view)) {
                if (message.getType() == MessageType.AliveReply) {
                    handleAliveReply((AliveReply) message, sender);
                }
                return;
            }

            // Use the message as heartbeat if the local process is
            // a follower and the sender is the leader of the current view
            if (sender == processDescriptor.getLeaderOfView(view)) {
                if (message.getType() == MessageType.Alive) {
                    Alive alive = (Alive) message;
                    if (alive.getView() != view) {
                        return;
                    }
                    lastHeartbeatRcvdTS = getTime();
                    observeFollowerHeartbeat(alive, sender);
                    if (alive.getHeartbeatId() >= 0) {
                        long calculatedHeartbeatInterval = getSuggestedHeartbeatIntervalForReply();
                        AliveReply reply = new AliveReply(alive.getView(), alive.getHeartbeatId(),
                                alive.getHeartbeatTimestamp(),
                                calculatedHeartbeatInterval);
                        network.sendMessage(reply, sender);
                    }
                } else {
                    lastHeartbeatRcvdTS = getTime();
                }
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // leader only.
            if (!processDescriptor.isLocalProcessLeader(view))
                return;

            // Ignore Alive messages, the clock was already reset when the
            // message was sent.
            if (message.getType() == MessageType.Alive) {
                return;
            }
            // If the message is not sent to all, ignore it as it is not useful
            // as an hearbeat. Use n-1 because a process does not send to self
            if (destinations.cardinality() < processDescriptor.numReplicas - 1) {
                return;
            }

            // Check if comment above is true
            assert !destinations.get(processDescriptor.localId) : message;

            // This process just sent a message to all. Reset the timeout.
            synchronized (ActiveFailureDetector.this) {
                lastHeartbeatSentTS = getTime();
                rescheduleAllFollowersLocked(lastHeartbeatSentTS);
                ActiveFailureDetector.this.notifyAll();
            }
        }
    }

    static long getTime() {
        // return System.currentTimeMillis();
        return System.nanoTime() / 1000000;
    }

    private static void validateTimeout(String timeoutName, int timeoutValue) {
        if (timeoutValue <= 0) {
            throw new IllegalArgumentException(timeoutName + " must be positive.");
        }
    }

    private void handleAliveReply(AliveReply reply, int sender) {
        synchronized (this) {
            if (reply.getView() != view) {
                return;
            }
            long sentTs = reply.getHeartbeatTimestamp();
            if (sentTs < 0) {
                return;
            }
            long now = getTime();
            long rtt = now - sentTs;
            if (rtt < 0) {
                return;
            }
            lastRttByFollower.put(sender, rtt);

            long heartbeatInterval = reply.getHeartbeatInterval();
            if (heartbeatInterval <= 0 || heartbeatInterval > Integer.MAX_VALUE) {
                logger.warn("Ignoring invalid heartbeat interval {} from replica {}",
                        heartbeatInterval, sender);
                return;
            }

            int newSendTimeout = (int) heartbeatInterval;
            Integer previousSendTimeout = perFollowerSendTimeouts.put(sender, newSendTimeout);
            perFollowerNextSendTs.put(sender, now + newSendTimeout);
            if (previousSendTimeout == null || previousSendTimeout.intValue() != newSendTimeout) {
                logger.info("Dynatune applied per-follower heartbeat interval: replica={} intervalMs={}",
                        sender, newSendTimeout);
                notifyAll();
            }
        }
    }

    private void observeFollowerHeartbeat(Alive alive, int leaderReplicaId) {
        synchronized (this) {
            if (alive.getRtt() >= 0) {
                observedRtts.addLast(alive.getRtt());
                trimWindow(observedRtts);
            }
            if (alive.getHeartbeatId() >= 0) {
                observedHeartbeatIds.add(alive.getHeartbeatId());
                while (observedHeartbeatIds.size() > processDescriptor.dynatuneMaxListSize) {
                    observedHeartbeatIds.pollFirst();
                }
            }
            updateFollowerTuning();
        }
    }

    private void resetLeaderObservations() {
        lastRttByFollower.clear();
        perFollowerSendTimeouts.clear();
        perFollowerNextSendTs.clear();
        nextHeartbeatIdByFollower.clear();
    }

    private long nextHeartbeatIdForFollowerLocked(int followerId) {
        Long nextHeartbeatId = nextHeartbeatIdByFollower.get(followerId);
        long heartbeatId = nextHeartbeatId == null ? 0L : nextHeartbeatId.longValue();
        nextHeartbeatIdByFollower.put(followerId, heartbeatId + 1L);
        return heartbeatId;
    }

    private Map<Integer, Alive> buildPerFollowerAlive(int logNextId, long heartbeatId,
                                                      int viewSnapshot) {
        Map<Integer, Alive> result = new HashMap<Integer, Alive>();
        for (int replicaId = 0; replicaId < processDescriptor.numReplicas; replicaId++) {
            if (replicaId == processDescriptor.localId) {
                continue;
            }
            result.put(replicaId,
                    createAliveForFollowerLocked(replicaId, logNextId, heartbeatId, viewSnapshot));
        }
        return result;
    }

    private void resetFollowerObservations() {
        observedRtts.clear();
        observedHeartbeatIds.clear();
        lastComputedEt = -1;
        lastSuggestedHeartbeatInterval = -1;
        followerTuningStarted = false;
    }

    private static <T> void trimWindow(ArrayDeque<T> window) {
        while (window.size() > processDescriptor.dynatuneMaxListSize) {
            window.removeFirst();
        }
    }

    private Alive createAliveForFollower(int followerId, int logNextId, long heartbeatId,
                                         int viewSnapshot) {
        synchronized (this) {
            return createAliveForFollowerLocked(followerId, logNextId, heartbeatId, viewSnapshot);
        }
    }

    private Alive createAliveForFollowerLocked(int followerId, int logNextId, long heartbeatId,
                                               int viewSnapshot) {
        long rttToEmbed = 0;
        Long lastRtt = lastRttByFollower.get(followerId);
        if (lastRtt != null && lastRtt.longValue() >= 0) {
            rttToEmbed = lastRtt.longValue();
        }
        long heartbeatIntervalToEmbed = getFollowerSendTimeoutLocked(followerId);
        return new Alive(viewSnapshot, logNextId, heartbeatId, rttToEmbed,
                heartbeatIntervalToEmbed);
    }

    private int getFollowerSendTimeoutLocked(int followerId) {
        Integer timeout = perFollowerSendTimeouts.get(followerId);
        if (timeout != null && timeout.intValue() > 0) {
            return timeout.intValue();
        }
        return sendTimeout;
    }

    private ArrayDeque<Integer> scheduleDueFollowersLocked(long now) {
        ArrayDeque<Integer> dueFollowers = new ArrayDeque<Integer>();
        for (int replicaId = 0; replicaId < processDescriptor.numReplicas; replicaId++) {
            if (replicaId == processDescriptor.localId) {
                continue;
            }
            Long nextSend = perFollowerNextSendTs.get(replicaId);
            if (nextSend == null) {
                nextSend = now;
            }
            if (nextSend <= now) {
                dueFollowers.addLast(replicaId);
            }
        }
        return dueFollowers;
    }

    private void markFollowerSentLocked(int followerId, long sendTs) {
        long interval = getFollowerSendTimeoutLocked(followerId);
        perFollowerNextSendTs.put(followerId, sendTs + interval);
    }

    private void rescheduleDefaultFollowersLocked(long now) {
        for (int replicaId = 0; replicaId < processDescriptor.numReplicas; replicaId++) {
            if (replicaId == processDescriptor.localId) {
                continue;
            }
            Integer override = perFollowerSendTimeouts.get(replicaId);
            if (override != null && override.intValue() > 0) {
                continue;
            }
            perFollowerNextSendTs.put(replicaId, now + sendTimeout);
        }
    }

    private void rescheduleAllFollowersLocked(long now) {
        for (int replicaId = 0; replicaId < processDescriptor.numReplicas; replicaId++) {
            if (replicaId == processDescriptor.localId) {
                continue;
            }
            perFollowerNextSendTs.put(replicaId, now + getFollowerSendTimeoutLocked(replicaId));
        }
    }

    private long getNextLeaderSendTimeLocked(long fallbackTs) {
        long nextSend = Long.MAX_VALUE;
        for (Long ts : perFollowerNextSendTs.values()) {
            if (ts.longValue() < nextSend) {
                nextSend = ts.longValue();
            }
        }
        if (nextSend == Long.MAX_VALUE) {
            return fallbackTs + sendTimeout;
        }
        return nextSend;
    }
    private long getSuggestedHeartbeatIntervalForReply() {
        synchronized (this) {
            if (lastSuggestedHeartbeatInterval > 0) {
                return lastSuggestedHeartbeatInterval;
            }
            // Keep default leader heartbeat interval until tuning has produced
            // a concrete suggestion.
            return -1;
        }
    }

    private void updateFollowerTuning() {
        if (!processDescriptor.dynatuneEnabled) {
            return;
        }
        int minListSize = processDescriptor.dynatuneMinListSize;
        if (observedRtts.size() < minListSize ||
            observedHeartbeatIds.size() < minListSize) {
            return;
        }
        double mean = computeMean(observedRtts);
        double stddev = computeStdDev(observedRtts, mean);
        double et = mean + processDescriptor.dynatuneSafetyFactor * stddev;
        int newSuspectTimeout = clampToPositiveIntCeil(et);
        int oldSuspectTimeout = suspectTimeout;
        if (newSuspectTimeout > 0 && newSuspectTimeout != oldSuspectTimeout) {
            setSuspectTimeout(newSuspectTimeout);
            logger.info("Dynatune updated E_t(suspectTimeout): oldMs={} newMs={} samples={}",
                    oldSuspectTimeout, newSuspectTimeout, observedRtts.size());
        }
        lastComputedEt = newSuspectTimeout;

        double packetLossRate = computePacketLossRate(observedHeartbeatIds);
        int suggestedInterval = computeSuggestedHeartbeatInterval(newSuspectTimeout, packetLossRate,
                processDescriptor.dynatuneHeartbeatProbability);
        int oldSuggestedInterval = lastSuggestedHeartbeatInterval;
        if (suggestedInterval > 0) {
            lastSuggestedHeartbeatInterval = suggestedInterval;
            if (suggestedInterval != oldSuggestedInterval) {
                logger.info(
                        "Dynatune updated suggested heartbeat interval: oldMs={} newMs={} lossRate={} etMs={}",
                        oldSuggestedInterval, suggestedInterval, packetLossRate, newSuspectTimeout);
            }
        }
    }

    private static double computeMean(ArrayDeque<Long> samples) {
        long sum = 0;
        for (Long sample : samples) {
            sum += sample.longValue();
        }
        return sum / (double) samples.size();
    }

    private static double computeStdDev(ArrayDeque<Long> samples, double mean) {
        if (samples.size() <= 1) {
            return 0.0;
        }
        double variance = 0.0;
        for (Long sample : samples) {
            double delta = sample.longValue() - mean;
            variance += delta * delta;
        }
        variance /= (samples.size() - 1);
        return Math.sqrt(variance);
    }

    private static double computePacketLossRate(NavigableSet<Long> heartbeatIds) {
        if (heartbeatIds.size() < 2) {
            return 0.0;
        }
        long first = heartbeatIds.first().longValue();
        long last = heartbeatIds.last().longValue();
        long expected = calculatePacketCount(first, last);
        long received = heartbeatIds.size();
        if (expected <= 0) {
            return 0.0;
        }
        double packetLossRate = 1.0 - (received / (double) expected);
        if (packetLossRate < 0.0) {
            return 0.0;
        }
        if (packetLossRate > 1.0) {
            return 1.0;
        }
        return packetLossRate;
    }

    private static int computeSuggestedHeartbeatInterval(double et, double packetLossRate,
                                                         double targetProbability) {
        if (et <= 0) {
            return -1;
        }
        double ceilLogTerm;
        if (packetLossRate <= 0.0) {
            ceilLogTerm = 1.0;
        } else if (packetLossRate >= 1.0) {
            return -1;
        } else {
            double logTerm = Math.log(1.0 - targetProbability) / Math.log(packetLossRate);
            ceilLogTerm = Math.ceil(logTerm);
        }
        double interval = Math.floor(et / (ceilLogTerm + 1.0));
        return clampToPositiveIntCeil(interval);
    }

    private static int clampToPositiveIntCeil(double value) {
        if (value <= 0) {
            return -1;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.ceil(value);
    }

    private static long calculatePacketCount(long firstId, long lastId) {
        if (lastId >= firstId) {
            return lastId - firstId + 1;
        }
        return (Long.MAX_VALUE - firstId) + lastId + 2;
    }

    private final static Logger logger = LoggerFactory.getLogger(ActiveFailureDetector.class);
}
