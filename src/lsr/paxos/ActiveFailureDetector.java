package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.PreVoteReply;
import lsr.paxos.messages.PreVoteRequest;
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

    private int view;

    /** Follower role: reception time of the last heartbeat from the leader */
    private volatile long lastHeartbeatRcvdTS;
    /** Follower role: whether this replica still has evidence of a live leader. */
    private boolean leaderKnown = true;
    /** Follower role: local backoff before starting the next pre-vote attempt. */
    private volatile long nextPreVoteNotBeforeTs;
    /** Leader role: time when the last message or heartbeat was sent to all */
    private volatile long lastHeartbeatSentTS;

    /** How long to wait for pre-vote replies before giving up (ms). */
    private static final int PRE_VOTE_TIMEOUT_MS = 1000;
    /** Pre-vote round state (guarded by synchronized(this)). */
    private long nextPreVoteRoundId = 0L;
    private long activePreVoteRoundId = -1L;
    private int activePreVoteView = -1;
    private final BitSet preVoteGranted = new BitSet();
    private final BitSet preVoteRejected = new BitSet();

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
        storage.addViewChangeListener(viewCahngeListener);
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

    public void setSuspectTimeout(int suspectTimeout) {
        validateTimeout("suspectTimeout", suspectTimeout);
        synchronized (this) {
            this.suspectTimeout = suspectTimeout;
            notifyAll();
        }
    }

    public void setSendTimeout(int sendTimeout) {
        validateTimeout("sendTimeout", sendTimeout);
        synchronized (this) {
            this.sendTimeout = sendTimeout;
            notifyAll();
        }
    }

    public void restoreDefaultTimeouts() {
        synchronized (this) {
            suspectTimeout = defaultSuspectTimeout;
            sendTimeout = defaultSendTimeout;
            notifyAll();
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
    protected Storage.ViewChangeListener viewCahngeListener = new Storage.ViewChangeListener() {

        public void viewChanged(int newView, int newLeader) {
            synchronized (ActiveFailureDetector.this) {
                logger.debug("FD has been informed about view {}", newView);
                view = newView;
                long now = getTime();
                lastHeartbeatRcvdTS = now;
                leaderKnown = true;
                nextPreVoteNotBeforeTs = now;
                activePreVoteRoundId = -1L;
                activePreVoteView = -1;
                preVoteGranted.clear();
                preVoteRejected.clear();
                ActiveFailureDetector.this.notifyAll();
            }
        }
    };

    public void run() {
        logger.info("Starting failure detector");
        try {
            // Warning for maintainers: Deadlock danger!!
            // The code below calls several methods in other classes while
            // holding the this lock.
            // If the methods called acquire locks and then try to call into
            // this failure detector,
            // there is the danger of deadlock. Therefore, always ensure that
            // the methods called
            // below do not themselves obtain locks.
            synchronized (this) {
                while (true) {
                    long now = getTime();
                    // Leader role
                    if (processDescriptor.isLocalProcessLeader(view)) {
                        // Send
                        Alive alive = new Alive(view, storage.getLog().getNextId());
                        network.sendToOthers(alive);
                        lastHeartbeatSentTS = now;
                        long nextSend = lastHeartbeatSentTS + sendTimeout;

                        while (now < nextSend && processDescriptor.isLocalProcessLeader(view)) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Sending next Alive in {} ms", nextSend - now);
                            }
                            wait(nextSend - now);
                            // recompute the state. lastHBSentTS might have
                            // changed.
                            now = getTime();
                            nextSend = lastHeartbeatSentTS + sendTimeout;
                        }
                        // Either no longer the leader or the it is time to send
                        // an hearbeat

                    } else {
                        // follower role
                        nextPreVoteNotBeforeTs = now;
                        long suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                        // Loop until either this process becomes the leader or
                        // until is time to suspect the leader
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
                            while (now < nextPreVoteNotBeforeTs &&
                                   !processDescriptor.isLocalProcessLeader(view)) {
                                wait(nextPreVoteNotBeforeTs - now);
                                now = getTime();
                                suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                                if (now < suspectTime) {
                                    break;
                                }
                            }
                            if (now < suspectTime) {
                                continue;
                            }
                            int preVoteView = view;
                            long heartbeatBeforePreVote = lastHeartbeatRcvdTS;
                            if (!runPreVoteRoundLocked(preVoteView)) {
                                if (view != preVoteView) {
                                    continue;
                                }
                                if (lastHeartbeatRcvdTS == heartbeatBeforePreVote) {
                                    leaderKnown = false;
                                }
                                nextPreVoteNotBeforeTs = getTime() + randomizedPreVoteBackoff();
                                continue;
                            }
                            // Raise the suspicion. A suspect task will be
                            // queued for execution
                            // on the Protocol thread.
                            fdListener.suspect(view);
                            // The view change is done asynchronously as seen
                            // from this thread.
                            // To avoid raising multiple suspicions, this thread
                            // suspends until
                            // the view change completes. When that happens, the
                            // method viewChange()
                            // will be called by the Protocol thread, which will
                            // notify() this
                            // monitor, thereby unlocking this thread.
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
            if (message.getType() == MessageType.PreVoteRequest) {
                handlePreVoteRequest((PreVoteRequest) message, sender);
                return;
            }
            if (message.getType() == MessageType.PreVoteReply) {
                handlePreVoteReply((PreVoteReply) message, sender);
                return;
            }

            // followers only.
            if (processDescriptor.isLocalProcessLeader(view))
                return;

            // Use the message as heartbeat if the local process is
            // a follower and the sender is the leader of the current view
            if (sender == processDescriptor.getLeaderOfView(view)) {
                long now = getTime();
                lastHeartbeatRcvdTS = now;
                leaderKnown = true;
                nextPreVoteNotBeforeTs = now;
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
            lastHeartbeatSentTS = getTime();
        }
    }

    private boolean runPreVoteRoundLocked(int suspectingView) throws InterruptedException {
        assert Thread.holdsLock(this);
        long roundId = ++nextPreVoteRoundId;
        activePreVoteRoundId = roundId;
        activePreVoteView = suspectingView;
        preVoteGranted.clear();
        preVoteRejected.clear();
        preVoteGranted.set(processDescriptor.localId);

        logger.info("JPAXOS_STARTING_PRE_VOTE localId={} view={} suspectedLeader={} " +
                    "suspectTimeoutMs={} preVoteTimeoutMs={}",
                processDescriptor.localId, suspectingView,
                processDescriptor.getLeaderOfView(suspectingView), suspectTimeout,
                PRE_VOTE_TIMEOUT_MS);

        PreVoteRequest request = new PreVoteRequest(suspectingView, roundId,
                processDescriptor.localId);
        network.sendToOthers(request);

        long deadline = getTime() + PRE_VOTE_TIMEOUT_MS;
        while (view == suspectingView && activePreVoteRoundId == roundId &&
               preVoteGranted.cardinality() < processDescriptor.majority &&
               preVoteRejected.cardinality() < processDescriptor.majority) {
            long remaining = deadline - getTime();
            if (remaining <= 0) {
                break;
            }
            wait(remaining);
        }

        boolean granted = view == suspectingView && activePreVoteRoundId == roundId &&
                          preVoteGranted.cardinality() >= processDescriptor.majority &&
                          preVoteRejected.cardinality() < processDescriptor.majority;
        logger.debug("Pre-vote round finished: view={} roundId={} granted={} grants={}/{} rejects={}/{}",
                suspectingView, roundId, granted, preVoteGranted.cardinality(),
                processDescriptor.majority, preVoteRejected.cardinality(),
                processDescriptor.majority);
        activePreVoteRoundId = -1L;
        activePreVoteView = -1;
        preVoteGranted.clear();
        preVoteRejected.clear();
        return granted;
    }

    private void handlePreVoteRequest(PreVoteRequest request, int sender) {
        boolean granted;
        synchronized (this) {
            granted = shouldGrantPreVoteLocked(request);
        }
        PreVoteReply reply = new PreVoteReply(request.getView(), request.getRoundId(), granted);
        network.sendMessage(reply, sender);
    }

    boolean shouldGrantPreVoteLocked(PreVoteRequest request) {
        assert Thread.holdsLock(this);
        if (request.getView() != view) {
            return false;
        }
        if (processDescriptor.isLocalProcessLeader(view)) {
            return false;
        }
        return !leaderKnown || getTime() - lastHeartbeatRcvdTS >= suspectTimeout;
    }

    private void handlePreVoteReply(PreVoteReply reply, int sender) {
        synchronized (this) {
            if (reply.getView() != view || reply.getView() != activePreVoteView) {
                return;
            }
            if (reply.getRoundId() != activePreVoteRoundId) {
                return;
            }
            if (!reply.isGranted()) {
                preVoteGranted.clear(sender);
                preVoteRejected.set(sender);
                if (preVoteRejected.cardinality() >= processDescriptor.majority) {
                    notifyAll();
                }
                return;
            }
            preVoteRejected.clear(sender);
            preVoteGranted.set(sender);
            if (preVoteGranted.cardinality() >= processDescriptor.majority) {
                notifyAll();
            }
        }
    }

    long randomizedPreVoteBackoff() {
        int base = Math.max(1, suspectTimeout);
        return base + ThreadLocalRandom.current().nextInt(base);
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

    private final static Logger logger = LoggerFactory.getLogger(ActiveFailureDetector.class);
}
