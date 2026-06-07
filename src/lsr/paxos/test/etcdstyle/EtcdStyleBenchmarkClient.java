package lsr.paxos.test.etcdstyle;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.paxos.test.etcdstyle.EtcdStyleProtocolCodec.ClientReplyFrame;

/**
 * Etcd-style benchmark client for JPaxos.
 *
 * Mirrors the external semantics of `etcdctl benchmark put` so JPaxos and
 * etcd/Raft results can be compared on equal footing.
 *
 * Semantics:
 *   --clients  : number of worker threads. Each worker keeps at most one
 *                request in flight, just like an etcd benchmark goroutine.
 *   --conns    : number of TCP connections to JPaxos. Workers map round-robin
 *                onto connections; multiple workers share the same TCP socket.
 *   --rate     : global request rate cap (rps). A shared paced limiter is
 *                used (analogous to golang.org/x/time/rate with burst=1).
 *   --total    : total number of requests to issue (rate * duration * 2 by
 *                convention, matching the etcd benchmark wrapper).
 *   --duration : fallback to derive --total when --total is not given.
 *
 *   The latency timer starts immediately before the request is handed off
 *   for sending and stops when the matching reply arrives. When configured,
 *   per-request timeout abandons a slow request so workers can continue.
 *
 * Output:
 *   Same shape as etcd benchmark (and as AsyncOpenLoopBenchmarkClient) so the
 *   existing analyze_benchmark_results.py and shard aggregator both parse it.
 */
public final class EtcdStyleBenchmarkClient {

    private static final class Args {
        String configPath;
        double rateRps;
        int durationSeconds;
        int clients = 1;
        int connections = 1;
        long totalRequests;
        int keySize = 8;
        int valSize = 256;
        // 0 disables per-request timeout (worker blocks forever on reply).
        long requestTimeoutMs = 0;
        // 0 disables drain bound (wait forever for in-flight requests to complete).
        long drainTimeoutMs = 0;
    }

    private static final class Sample {
        final long wallMs;
        final long latencyNs;
        final int sent;
        final int success;
        final int timeout;
        final int error;

        Sample(long wallMs, long latencyNs, int sent, int success, int timeout, int error) {
            this.wallMs = wallMs;
            this.latencyNs = latencyNs;
            this.sent = sent;
            this.success = success;
            this.timeout = timeout;
            this.error = error;
        }
    }

    /** Handle for an in-flight request so the worker can abandon it on timeout. */
    private static final class Inflight {
        final Connection connection;
        final int seq;
        final CompletableFuture<Boolean> future;

        Inflight(Connection connection, int seq, CompletableFuture<Boolean> future) {
            this.connection = connection;
            this.seq = seq;
            this.future = future;
        }
    }

    /**
     * Token-bucket-like paced limiter. Each acquire() reserves the next slot
     * on a monotonically advancing schedule, then sleeps until that instant.
     * Average throughput across all callers is bounded by rateRps.
     */
    private static final class PacedLimiter {
        private final long intervalNs;
        private final AtomicLong nextNs;

        PacedLimiter(double rateRps, long startNs) {
            if (rateRps <= 0) {
                throw new IllegalArgumentException("rate must be > 0");
            }
            this.intervalNs = (long) Math.max(1, Math.floor(1_000_000_000.0 / rateRps));
            this.nextNs = new AtomicLong(startNs);
        }

        void acquire() {
            long slot = nextNs.getAndAdd(intervalNs);
            while (true) {
                long now = System.nanoTime();
                long wait = slot - now;
                if (wait <= 0) {
                    return;
                }
                if (wait > 2_000_000L) {
                    LockSupport.parkNanos(wait - 1_000_000L);
                } else {
                    LockSupport.parkNanos(wait);
                }
            }
        }
    }

    /**
     * Pipelined connection. Many workers may share one connection: each
     * request gets a fresh sequence id and a future that the reader thread
     * completes upon receiving the matching reply.
     */
    private static final class Connection implements AutoCloseable {
        final int index;
        private final PID replica;
        private final SocketChannel channel;
        private final long clientId;
        private final AtomicInteger seqGen = new AtomicInteger(0);
        private final ConcurrentHashMap<Integer, CompletableFuture<Boolean>> pending = new ConcurrentHashMap<>();
        private final Object writeLock = new Object();
        private final Thread reader;
        private volatile boolean closed = false;
        private volatile IOException failure;

        Connection(int index, PID replica) throws IOException {
            this.index = index;
            this.replica = replica;
            this.channel = SocketChannel.open();
            channel.configureBlocking(true);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setReuseAddress(true);
            channel.connect(new InetSocketAddress(replica.getHostname(), replica.getClientPort()));

            ByteBuffer init = ByteBuffer.allocate(1);
            init.put(EtcdStyleProtocolCodec.REQUEST_NEW_ID);
            init.flip();
            while (init.hasRemaining()) {
                channel.write(init);
            }

            ByteBuffer idBuf = ByteBuffer.allocate(8);
            while (idBuf.hasRemaining()) {
                int n = channel.read(idBuf);
                if (n < 0) {
                    throw new EOFException("replica closed during client id init: " + replica);
                }
            }
            idBuf.flip();
            this.clientId = idBuf.getLong();

            this.reader = new Thread(this::runReader, "EtcdStyleClient-reader-" + index);
            this.reader.setDaemon(true);
            this.reader.start();
        }

        /** Send a request and return a handle whose future completes when the reply arrives. */
        Inflight send(byte[] payload) throws IOException {
            int seq = seqGen.incrementAndGet();
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            pending.put(seq, future);
            ByteBuffer frame = EtcdStyleProtocolCodec.encodeRequest(clientId, seq, payload);
            try {
                synchronized (writeLock) {
                    while (frame.hasRemaining()) {
                        channel.write(frame);
                    }
                }
            } catch (IOException e) {
                pending.remove(seq);
                future.completeExceptionally(e);
                throw e;
            }
            return new Inflight(this, seq, future);
        }

        /** Abandon a request locally so a late reply (or none at all) does not leak memory. */
        void cancel(int seq) {
            pending.remove(seq);
        }

        private void runReader() {
            ByteBuffer buf = ByteBuffer.allocate(1 << 20);
            try {
                while (!closed) {
                    int n = channel.read(buf);
                    if (n < 0) {
                        throw new EOFException("replica closed: " + replica);
                    }
                    if (n == 0) {
                        continue;
                    }
                    buf.flip();
                    while (true) {
                        ClientReplyFrame frame = EtcdStyleProtocolCodec.tryDecodeClientReply(buf);
                        if (frame == null) {
                            break;
                        }
                        if (frame.result == EtcdStyleProtocolCodec.REPLY_OK) {
                            CompletableFuture<Boolean> f = pending.remove(frame.sequenceId);
                            if (f != null) {
                                f.complete(Boolean.TRUE);
                            }
                        } else {
                            // Non-OK frames carry no usable seq id; surface as a single error
                            // by failing one arbitrary pending future. Empirically these do
                            // not occur in normal benchmark runs.
                            Map.Entry<Integer, CompletableFuture<Boolean>> any =
                                    pending.entrySet().stream().findFirst().orElse(null);
                            if (any != null && pending.remove(any.getKey(), any.getValue())) {
                                any.getValue().completeExceptionally(
                                        new IOException("non-OK reply: result=" + frame.result));
                            }
                        }
                    }
                    buf.compact();
                }
            } catch (IOException e) {
                if (!closed) {
                    failure = e;
                }
            } finally {
                IOException cause = failure != null
                        ? failure
                        : new IOException("connection closed");
                for (Map.Entry<Integer, CompletableFuture<Boolean>> e : pending.entrySet()) {
                    if (pending.remove(e.getKey(), e.getValue())) {
                        e.getValue().completeExceptionally(cause);
                    }
                }
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            try {
                reader.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args;
        try {
            args = parseArgs(argv);
        } catch (RuntimeException e) {
            System.err.println("ERROR: " + e.getMessage());
            usage();
            System.exit(2);
            return;
        }

        Configuration configuration = new Configuration(args.configPath);
        List<PID> replicas = configuration.getProcesses();
        if (replicas.isEmpty()) {
            throw new IllegalArgumentException("configuration contains no replicas");
        }

        long totalRequests = args.totalRequests > 0
                ? args.totalRequests
                : (long) Math.ceil(args.rateRps * args.durationSeconds);

        int connectionCount = Math.max(1, args.connections);
        int workerCount = Math.max(1, args.clients);

        System.err.printf(Locale.US,
                "Etcd-style benchmark: rate=%.2f rps, clients=%d, conns=%d, total=%d, key/val=%d/%d%n",
                args.rateRps, workerCount, connectionCount, totalRequests,
                args.keySize, args.valSize);

        List<Connection> connections = new ArrayList<>(connectionCount);
        try {
            for (int i = 0; i < connectionCount; i++) {
                PID replica = replicas.get(i % replicas.size());
                connections.add(new Connection(i, replica));
            }

            byte[] payload = new byte[args.keySize + args.valSize];
            new Random(0xE7CD51E1L).nextBytes(payload);

            // Counters
            AtomicLong remaining = new AtomicLong(totalRequests);
            AtomicLong sent = new AtomicLong(0);
            AtomicLong success = new AtomicLong(0);
            AtomicLong errors = new AtomicLong(0);
            AtomicLong timeouts = new AtomicLong(0);
            // Each request can produce one send event and one outcome event.
            long eventCapacity = totalRequests > Integer.MAX_VALUE / 2L
                    ? Integer.MAX_VALUE
                    : Math.max(totalRequests, totalRequests * 2);
            Sample[] samples = new Sample[(int) eventCapacity];
            AtomicInteger sampleIdx = new AtomicInteger(0);

            long startNs = System.nanoTime();
            long startWallMs = System.currentTimeMillis();
            PacedLimiter limiter = new PacedLimiter(args.rateRps, startNs);

            final long perReqTimeoutMs = args.requestTimeoutMs;

            CountDownLatch done = new CountDownLatch(workerCount);
            Thread[] workers = new Thread[workerCount];
            for (int w = 0; w < workerCount; w++) {
                final Connection conn = connections.get(w % connectionCount);
                Thread t = new Thread(() -> {
                    try {
                        while (remaining.getAndDecrement() > 0) {
                            limiter.acquire();
                            long reqStartWallMs = System.currentTimeMillis();
                            long reqStartNs = System.nanoTime();
                            Inflight ifl = null;
                            try {
                                ifl = conn.send(payload);
                                sent.incrementAndGet();
                                addSample(samples, sampleIdx,
                                        new Sample(reqStartWallMs, 0L, 1, 0, 0, 0));
                                if (perReqTimeoutMs > 0) {
                                    ifl.future.get(perReqTimeoutMs, TimeUnit.MILLISECONDS);
                                } else {
                                    ifl.future.get();
                                }
                                long latencyNs = System.nanoTime() - reqStartNs;
                                addSample(samples, sampleIdx,
                                        new Sample(System.currentTimeMillis(), latencyNs, 0, 1, 0, 0));
                                success.incrementAndGet();
                            } catch (TimeoutException te) {
                                // Abandon the request so a late reply does not leak,
                                // and free the worker for its next request.
                                if (ifl != null) {
                                    ifl.connection.cancel(ifl.seq);
                                }
                                addSample(samples, sampleIdx,
                                        new Sample(System.currentTimeMillis(), 0L, 0, 0, 1, 0));
                                timeouts.incrementAndGet();
                            } catch (IOException | ExecutionException e) {
                                addSample(samples, sampleIdx,
                                        new Sample(System.currentTimeMillis(), 0L, 0, 0, 0, 1));
                                errors.incrementAndGet();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    } finally {
                        done.countDown();
                    }
                }, "EtcdStyleClient-worker-" + w);
                t.setDaemon(true);
                workers[w] = t;
                t.start();
            }

            // Wait for workers to finish issuing all requests and draining their
            // last in-flight reply. If a worker is stuck (e.g. per-request timeout
            // disabled and a reply never arrives), the drain bound prevents the
            // benchmark from hanging the surrounding sweep.
            double estimatedRunSecs = totalRequests / Math.max(1.0, args.rateRps);
            long maxRunBudgetMs = Math.max((long) (estimatedRunSecs * 4_000.0), 60_000L);
            long awaitMs = args.drainTimeoutMs > 0
                    ? maxRunBudgetMs + args.drainTimeoutMs
                    : 0L;
            if (awaitMs > 0) {
                if (!done.await(awaitMs, TimeUnit.MILLISECONDS)) {
                    System.err.println("WARN: drain timeout exceeded; interrupting workers");
                    for (Thread t : workers) {
                        t.interrupt();
                    }
                    done.await(2, TimeUnit.SECONDS);
                }
            } else {
                done.await();
            }
            long endWallMs = System.currentTimeMillis();

            for (Connection c : connections) {
                c.close();
            }

            int n = Math.min(sampleIdx.get(), samples.length);
            List<Sample> all = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                if (samples[i] != null) {
                    all.add(samples[i]);
                }
            }

            printResults(args, all, startWallMs, endWallMs,
                    sent.get(), success.get(), errors.get(), timeouts.get());
        } finally {
            for (Connection c : connections) {
                try {
                    c.close();
                } catch (RuntimeException ignored) {
                }
            }
        }
    }

    private static void addSample(Sample[] samples, AtomicInteger sampleIdx, Sample sample) {
        int idx = sampleIdx.getAndIncrement();
        if (idx < samples.length) {
            samples[idx] = sample;
        }
    }

    private static void printResults(Args args, List<Sample> samples,
                                     long startWallMs, long endWallMs,
                                     long sent, long success, long errors, long timeouts) {
        List<Sample> successfulSamples = new ArrayList<>();
        for (Sample s : samples) {
            if (s.success > 0) {
                successfulSamples.add(s);
            }
        }

        int count = successfulSamples.size();
        double totalSecs = (endWallMs - startWallMs) / 1000.0;
        if (totalSecs <= 0) {
            totalSecs = 1e-9;
        }

        long[] latNs = new long[count];
        for (int i = 0; i < count; i++) {
            latNs[i] = successfulSamples.get(i).latencyNs;
        }
        Arrays.sort(latNs);

        double slowest = count == 0 ? 0.0 : latNs[count - 1] / 1e9;
        double fastest = count == 0 ? 0.0 : latNs[0] / 1e9;
        double avg = 0.0;
        double stddev = 0.0;
        if (count > 0) {
            double sum = 0.0;
            for (long l : latNs) {
                sum += l / 1e9;
            }
            avg = sum / count;
            double var = 0.0;
            for (long l : latNs) {
                double d = (l / 1e9) - avg;
                var += d * d;
            }
            stddev = Math.sqrt(var / count);
        }
        double rps = count / totalSecs;
        double sentRps = sent / totalSecs;

        System.out.println("Summary:");
        System.out.printf(Locale.US, "  Total:\t%.4f secs.%n", totalSecs);
        System.out.printf(Locale.US, "  Slowest:\t%.6f secs.%n", slowest);
        System.out.printf(Locale.US, "  Fastest:\t%.6f secs.%n", fastest);
        System.out.printf(Locale.US, "  Average:\t%.6f secs.%n", avg);
        System.out.printf(Locale.US, "  Stddev:\t%.6f secs.%n", stddev);
        System.out.printf(Locale.US, "  Requests/sec:\t%.2f%n", rps);
        System.out.printf(Locale.US, "  Sent Requests/sec:\t%.2f%n", sentRps);
        System.out.printf(Locale.US, "  Sent:\t%d%n", sent);
        System.out.printf(Locale.US, "  Success:\t%d%n", success);
        System.out.printf(Locale.US, "  Timeouts:\t%d%n", timeouts);
        System.out.printf(Locale.US, "  Dropped:\t%d%n", 0L);
        System.out.printf(Locale.US, "  Errors:\t%d%n", errors);
        System.out.println();

        System.out.println("Latency distribution:");
        if (count > 0) {
            for (double p : new double[]{10, 25, 50, 75, 90, 95, 99, 99.9}) {
                System.out.printf(Locale.US, "  %s%% in %.6f secs.%n",
                        formatPct(p), percentileSeconds(latNs, p));
            }
        }
        System.out.println();

        System.out.println("Sample in one second:");
        System.out.println("  UNIX-SECOND,min_latency_ms,avg_latency_ms,max_latency_ms,avg_throughput,target_rps,sent,success,timeout,error,dropped");
        printPerSecond(samples, args.rateRps);
        System.out.println();
    }

    private static String formatPct(double p) {
        if (p == Math.floor(p)) {
            return String.format(Locale.US, "%d", (long) p);
        }
        return String.format(Locale.US, "%.1f", p);
    }

    private static double percentileSeconds(long[] sortedLatenciesNs, double p) {
        if (sortedLatenciesNs.length == 0) {
            return 0.0;
        }
        double rank = (p / 100.0) * (sortedLatenciesNs.length - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo == hi) {
            return sortedLatenciesNs[lo] / 1e9;
        }
        double weight = rank - lo;
        return ((1.0 - weight) * sortedLatenciesNs[lo] + weight * sortedLatenciesNs[hi]) / 1e9;
    }

    private static void printPerSecond(List<Sample> samples, double targetRps) {
        // Keyed by event wall-clock second: sends at send time, successes at
        // completion time, and failed outcomes at failure time.
        TreeMap<Long, long[]> bySecond = new TreeMap<>();
        for (Sample s : samples) {
            long sec = s.wallMs / 1000L;
            long[] v = bySecond.get(sec);
            if (v == null) {
                // [success, sumNs, minNs, maxNs, sent, timeout, error]
                v = new long[]{0, 0, Long.MAX_VALUE, 0, 0, 0, 0};
                bySecond.put(sec, v);
            }
            v[4] += s.sent;
            v[5] += s.timeout;
            v[6] += s.error;
            if (s.success > 0) {
                v[0] += s.success;
                v[1] += s.latencyNs;
                if (s.latencyNs < v[2]) v[2] = s.latencyNs;
                if (s.latencyNs > v[3]) v[3] = s.latencyNs;
            }
        }
        for (Map.Entry<Long, long[]> e : bySecond.entrySet()) {
            long[] v = e.getValue();
            long successCount = v[0];
            double minMs = successCount == 0 ? 0.0 : v[2] / 1e6;
            double avgMs = successCount == 0 ? 0.0 : (v[1] / (double) successCount) / 1e6;
            double maxMs = successCount == 0 ? 0.0 : v[3] / 1e6;
            System.out.printf(Locale.US,
                    "  %d,%.2f,%.2f,%.2f,%d,%.2f,%d,%d,%d,%d,%d%n",
                    e.getKey(), minMs, avgMs, maxMs, successCount,
                    targetRps, v[4], successCount, v[5], v[6], 0L);
        }
    }

    private static Args parseArgs(String[] argv) {
        Args args = new Args();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            String next = (i + 1 < argv.length) ? argv[i + 1] : null;
            switch (a) {
                case "--config":              args.configPath = next; i++; break;
                case "--rate":                args.rateRps = Double.parseDouble(next); i++; break;
                case "--duration":            args.durationSeconds = Integer.parseInt(next); i++; break;
                case "--clients":             args.clients = Integer.parseInt(next); i++; break;
                case "--conns":
                case "--connections":         args.connections = Integer.parseInt(next); i++; break;
                case "--total":               args.totalRequests = Long.parseLong(next); i++; break;
                case "--key-size":            args.keySize = Integer.parseInt(next); i++; break;
                case "--val-size":            args.valSize = Integer.parseInt(next); i++; break;
                // The benchmark.sh wrapper passes these options; honored where supported.
                case "--request-timeout-ms":
                    args.requestTimeoutMs = Long.parseLong(next); i++; break;
                case "--timeout":
                    args.requestTimeoutMs = (long) (Double.parseDouble(next) * 1000.0); i++; break;
                case "--drain-timeout":
                    args.drainTimeoutMs = (long) (Double.parseDouble(next) * 1000.0); i++; break;
                // Ignored options (kept for CLI compatibility with AsyncOpenLoopBenchmarkClient).
                case "--max-inflight":
                case "--max-queued-bytes":
                case "--workers":
                    i++; break;
                default:
                    throw new IllegalArgumentException("unknown argument: " + a);
            }
        }
        if (args.configPath == null) {
            throw new IllegalArgumentException("--config is required");
        }
        if (args.rateRps <= 0) {
            throw new IllegalArgumentException("--rate must be > 0");
        }
        if (args.durationSeconds <= 0 && args.totalRequests <= 0) {
            throw new IllegalArgumentException("either --duration or --total must be > 0");
        }
        if (args.clients <= 0) {
            throw new IllegalArgumentException("--clients must be > 0");
        }
        if (args.connections <= 0) {
            throw new IllegalArgumentException("--connections must be > 0");
        }
        if (args.keySize < 0 || args.valSize < 0 || args.keySize + args.valSize <= 0) {
            throw new IllegalArgumentException("--key-size + --val-size must be > 0");
        }
        if (args.requestTimeoutMs < 0) {
            throw new IllegalArgumentException("--request-timeout-ms must be >= 0");
        }
        if (args.drainTimeoutMs < 0) {
            throw new IllegalArgumentException("--drain-timeout must be >= 0");
        }
        return args;
    }

    private static void usage() {
        System.err.println(EtcdStyleBenchmarkClient.class.getName() + " \\");
        System.err.println("  --config <paxos.properties> \\");
        System.err.println("  --rate <rps> --clients <n> [--connections <n>] \\");
        System.err.println("  ( --total <count> | --duration <seconds> ) \\");
        System.err.println("  [--key-size <bytes>] [--val-size <bytes>]");
    }

    private EtcdStyleBenchmarkClient() {
    }
}
