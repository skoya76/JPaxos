package lsr.paxos.test;

import lsr.common.Configuration;
import lsr.common.ClientCommand;
import lsr.common.ClientCommand.CommandType;
import lsr.common.ClientReply;
import lsr.common.ClientRequest;
import lsr.common.PID;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.client.Client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop throughput/latency benchmark for JPaxos EchoService.
 *
 * Produces output in the same format as etcdctl benchmark so that
 * analyze_benchmark_results.py (from raft/static-network-no-failure) can
 * be used without modification.
 *
 * Open-loop design:
 *   A scheduler thread fires requests at exactly --rate req/s regardless of
 *   whether previous requests have completed.  A small pool of pipelined TCP
 *   connections carries up to --clients in-flight requests.  Replies are
 *   matched by RequestId, so a single connection can have many outstanding
 *   requests.  If the in-flight limit is full (latency is backing up faster
 *   than requests complete), the scheduler records a dropped request and moves
 *   on — it does NOT slow down.  This keeps the offered load independent from
 *   the server's response rate.
 *
 * Usage:
 *   java -cp ... lsr.paxos.test.BenchmarkClient \
 *       --config paxos.properties \
 *       --rate   5000  \
 *       --duration 10  \
 *       --clients  20000 \
 *       --connections 64 \
 *       --key-size  8  \
 *       --val-size 256
 */
public class BenchmarkClient {

    static int maxInflight  = 200;
    static int connections  = 64;
    static int targetRate   = 1000;
    static int durationSecs = 10;
    static int keySize      = 8;
    static int valSize      = 256;
    static String configFile = "paxos.properties";
    static int drainTimeoutSecs = 5;

    static class Sample {
        final long sendMs;
        final long latencyNs;
        Sample(long sendMs, long latencyNs) {
            this.sendMs    = sendMs;
            this.latencyNs = latencyNs;
        }
    }

    static class Pending {
        final long sendMs;
        final long sendNs;

        Pending(long sendMs, long sendNs) {
            this.sendMs = sendMs;
            this.sendNs = sendNs;
        }
    }

    static class PipelinedConnection implements AutoCloseable {
        private final PID replica;
        private final ConcurrentLinkedQueue<Sample> samples;
        private final AtomicLong errors;
        private final Semaphore inflightPermits;
        private final AtomicInteger outstanding;
        private final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger sequenceId = new AtomicInteger(0);

        private Socket socket;
        private DataOutputStream output;
        private DataInputStream input;
        private Thread readerThread;
        private long clientId;

        PipelinedConnection(PID replica, ConcurrentLinkedQueue<Sample> samples,
                            AtomicLong errors, Semaphore inflightPermits,
                            AtomicInteger outstanding) {
            this.replica = replica;
            this.samples = samples;
            this.errors = errors;
            this.inflightPermits = inflightPermits;
            this.outstanding = outstanding;
        }

        void connect() throws IOException {
            socket = new Socket(replica.getHostname(), replica.getClientPort());
            socket.setReuseAddress(true);
            socket.setTcpNoDelay(true);
            output = new DataOutputStream(socket.getOutputStream());
            input = new DataInputStream(socket.getInputStream());

            output.write(Client.REQUEST_NEW_ID);
            output.flush();
            clientId = input.readLong();

            readerThread = new Thread(this::readReplies, "BenchmarkClient-reader-" + clientId);
            readerThread.setDaemon(true);
            readerThread.start();
        }

        void send(byte[] payload, long sendMs, long sendNs) throws IOException {
            int seqNo = sequenceId.incrementAndGet();
            ClientRequest request = new ClientRequest(new RequestId(clientId, seqNo), payload);
            ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
            ByteBuffer bb = ByteBuffer.allocate(command.byteSize());
            command.writeTo(bb);

            pending.put(seqNo, new Pending(sendMs, sendNs));
            outstanding.incrementAndGet();

            try {
                synchronized (output) {
                    output.write(bb.array());
                    output.flush();
                }
            } catch (IOException e) {
                if (pending.remove(seqNo) != null) {
                    outstanding.decrementAndGet();
                }
                throw e;
            }
        }

        private void readReplies() {
            while (running.get()) {
                try {
                    ClientReply clientReply = new ClientReply(input);
                    if (clientReply.getResult() != ClientReply.Result.OK) {
                        errors.incrementAndGet();
                        continue;
                    }

                    Reply reply = new Reply(clientReply.getValue());
                    int seqNo = reply.getRequestId().getSeqNumber();
                    Pending p = pending.remove(seqNo);
                    if (p == null) {
                        errors.incrementAndGet();
                        continue;
                    }

                    samples.add(new Sample(p.sendMs, System.nanoTime() - p.sendNs));
                    outstanding.decrementAndGet();
                    inflightPermits.release();
                } catch (IOException e) {
                    if (running.get()) {
                        errors.incrementAndGet();
                    }
                    break;
                }
            }
        }

        int failPending() {
            int failed = pending.size();
            for (Integer seqNo : pending.keySet()) {
                Pending removed = pending.remove(seqNo);
                if (removed != null) {
                    outstanding.decrementAndGet();
                    inflightPermits.release();
                }
            }
            return failed;
        }

        public void close() {
            running.set(false);
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException ignored) {
            }
            if (readerThread != null) {
                try {
                    readerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        parseArgs(args);

        if (connections < 1) {
            connections = 1;
        }
        if (connections > maxInflight) {
            connections = maxInflight;
        }

        System.err.printf("Open-loop pipelined benchmark: rate=%d rps  maxInflight=%d  connections=%d  duration=%ds%n",
                targetRate, maxInflight, connections, durationSecs);

        Configuration config = new Configuration(configFile);
        List<PID> replicas = config.getProcesses();
        if (replicas.isEmpty()) {
            throw new IllegalArgumentException("No replicas found in " + configFile);
        }

        Semaphore inflightPermits = new Semaphore(maxInflight);
        ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
        AtomicLong dropped = new AtomicLong(0);
        AtomicLong errors  = new AtomicLong(0);
        AtomicInteger outstanding = new AtomicInteger(0);

        List<PipelinedConnection> conns = new ArrayList<>();
        for (int i = 0; i < connections; i++) {
            PID replica = replicas.get(i % replicas.size());
            PipelinedConnection conn = new PipelinedConnection(
                    replica, samples, errors, inflightPermits, outstanding);
            conn.connect();
            conns.add(conn);
        }

        byte[] payload = new byte[keySize + valSize];
        new Random().nextBytes(payload);

        long intervalNs  = 1_000_000_000L / targetRate;
        long nextFireNs  = System.nanoTime();
        long benchStartMs = System.currentTimeMillis();
        long endMs        = benchStartMs + (long) durationSecs * 1000;

        // Scheduler loop: fire at exactly targetRate/s, fire-and-forget.
        int nextConnection = 0;
        while (true) {
            long nowNs = System.nanoTime();
            if (nowNs < nextFireNs) {
                LockSupport.parkNanos(nextFireNs - nowNs);
                continue;
            }
            if (System.currentTimeMillis() >= endMs) break;

            nextFireNs += intervalNs;

            // Non-blocking acquire: if the pipeline is full, count as dropped.
            if (!inflightPermits.tryAcquire()) {
                dropped.incrementAndGet();
                continue;
            }

            long sendMs = System.currentTimeMillis();
            long sendNs = System.nanoTime();
            PipelinedConnection conn = conns.get(nextConnection);
            nextConnection = (nextConnection + 1) % conns.size();
            try {
                conn.send(payload, sendMs, sendNs);
            } catch (IOException e) {
                errors.incrementAndGet();
                inflightPermits.release();
            }
        }

        long benchEndMs = System.currentTimeMillis();

        // Give in-flight requests up to 5 s to drain before printing results.
        long drainDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(drainTimeoutSecs);
        while (outstanding.get() > 0 && System.nanoTime() < drainDeadlineNs) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        for (PipelinedConnection conn : conns) {
            errors.addAndGet(conn.failPending());
            conn.close();
        }

        List<Sample> all = new ArrayList<>(samples);
        printResults(all, benchStartMs, benchEndMs, dropped.get(), errors.get());
    }

    // -----------------------------------------------------------------------
    // Output
    // -----------------------------------------------------------------------

    static void printResults(List<Sample> samples, long startMs, long endMs,
                             long dropped, long errors) {
        int count = samples.size();
        if (count == 0) {
            System.out.println("No requests completed.");
            return;
        }

        long[] latNs = new long[count];
        for (int i = 0; i < count; i++) latNs[i] = samples.get(i).latencyNs;
        Arrays.sort(latNs);

        double totalSecs = (endMs - startMs) / 1000.0;
        double slowest   = latNs[count - 1] / 1e9;
        double fastest   = latNs[0] / 1e9;
        double rps       = count / totalSecs;

        double sumNs = 0;
        for (long l : latNs) sumNs += l;
        double average = sumNs / count / 1e9;

        double meanNs = sumNs / count;
        double variance = 0;
        for (long l : latNs) variance += (l - meanNs) * (l - meanNs);
        double stddev = Math.sqrt(variance / count) / 1e9;

        System.out.printf("Summary:%n");
        System.out.printf("  Total:\t%.4f secs.%n",    totalSecs);
        System.out.printf("  Slowest:\t%.6f secs.%n",  slowest);
        System.out.printf("  Fastest:\t%.6f secs.%n",  fastest);
        System.out.printf("  Average:\t%.6f secs.%n",  average);
        System.out.printf("  Stddev:\t%.6f secs.%n",   stddev);
        System.out.printf("  Requests/sec:\t%.2f%n",   rps);
        System.out.printf("  Dropped:\t%d%n",           dropped);
        System.out.printf("  Errors:\t%d%n%n",          errors);

        System.out.println("Response time histogram:");
        printHistogram(latNs);
        System.out.println();

        System.out.println("Latency distribution:");
        for (int p : new int[]{10, 50, 90, 95, 99}) {
            System.out.printf("  %d%% in %.6f secs.%n", p,
                    latNs[percentileIndex(count, p / 100.0)] / 1e9);
        }
        System.out.printf("  99.9%% in %.6f secs.%n%n",
                latNs[percentileIndex(count, 0.999)] / 1e9);

        System.out.println("Sample in one second:");
        System.out.println("  UNIX-SECOND,min_latency_ms,avg_latency_ms,max_latency_ms,avg_throughput");
        printPerSecond(samples);
        System.out.println();
    }

    static int percentileIndex(int count, double fraction) {
        int idx = (int) Math.ceil(fraction * count) - 1;
        return Math.max(0, Math.min(idx, count - 1));
    }

    static void printHistogram(long[] sortedNs) {
        int count = sortedNs.length;
        long min  = sortedNs[0];
        long max  = sortedNs[count - 1];
        final int NUM_BUCKETS = 10;
        final int BAR_WIDTH   = 40;

        if (min == max) {
            System.out.printf("  %.6f [%d]\t|%n", min / 1e9, count);
            return;
        }

        double width = (double)(max - min) / NUM_BUCKETS;
        int[] counts = new int[NUM_BUCKETS];
        for (long ns : sortedNs) {
            int b = (int)((ns - min) / width);
            counts[Math.min(b, NUM_BUCKETS - 1)]++;
        }

        int maxCount = 0;
        for (int c : counts) if (c > maxCount) maxCount = c;

        for (int i = 0; i < NUM_BUCKETS; i++) {
            double bound = (min + width * (i + 1)) / 1e9;
            int barLen   = maxCount > 0 ? counts[i] * BAR_WIDTH / maxCount : 0;
            StringBuilder bar = new StringBuilder();
            for (int j = 0; j < barLen; j++) bar.append('|');
            System.out.printf("  %.6f [%d]\t|%s%n", bound, counts[i], bar);
        }
    }

    static void printPerSecond(List<Sample> samples) {
        TreeMap<Long, long[]> bySecond = new TreeMap<>();
        for (Sample s : samples) {
            long sec = s.sendMs / 1000;
            long[] v = bySecond.get(sec);
            if (v == null) {
                bySecond.put(sec, new long[]{1, s.latencyNs, s.latencyNs, s.latencyNs});
            } else {
                v[0]++;
                v[1] += s.latencyNs;
                if (s.latencyNs < v[2]) v[2] = s.latencyNs;
                if (s.latencyNs > v[3]) v[3] = s.latencyNs;
            }
        }
        for (Map.Entry<Long, long[]> e : bySecond.entrySet()) {
            long[] v = e.getValue();
            System.out.printf("  %d,%.2f,%.2f,%.2f,%d%n",
                    e.getKey(),
                    v[2] / 1e6,
                    (v[1] / v[0]) / 1e6,
                    v[3] / 1e6,
                    v[0]);
        }
    }

    static void parseArgs(String[] args) {
        for (int i = 0; i + 1 < args.length; i++) {
            switch (args[i]) {
                case "--config":   configFile   = args[++i]; break;
                case "--rate":     targetRate   = Integer.parseInt(args[++i]); break;
                case "--duration": durationSecs = Integer.parseInt(args[++i]); break;
                case "--clients":  maxInflight  = Integer.parseInt(args[++i]); break;
                case "--connections":
                    connections = Integer.parseInt(args[++i]);
                    break;
                case "--drain-timeout":
                    drainTimeoutSecs = Integer.parseInt(args[++i]);
                    break;
                case "--key-size": keySize      = Integer.parseInt(args[++i]); break;
                case "--val-size": valSize      = Integer.parseInt(args[++i]); break;
                default:
                    System.err.printf("Unknown option: %s%n", args[i]);
            }
        }
    }
}
