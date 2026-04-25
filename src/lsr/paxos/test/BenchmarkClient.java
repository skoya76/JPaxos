package lsr.paxos.test;

import lsr.common.Configuration;
import lsr.paxos.client.Client;
import lsr.paxos.client.ReplicationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 *   whether previous requests have completed.  A pool of --clients pre-connected
 *   Client objects handles the requests concurrently.  If every client is busy
 *   (latency is backing up faster than requests complete), the scheduler records
 *   a dropped request and moves on — it does NOT slow down.  This decouples the
 *   offered load from actual throughput and produces the hockey-stick curve where
 *   throughput plateaus and latency spikes when the system is overloaded.
 *
 * Usage:
 *   java -cp ... lsr.paxos.test.BenchmarkClient \
 *       --config paxos.properties \
 *       --rate   5000  \
 *       --duration 10  \
 *       --clients  200  \
 *       --key-size  8  \
 *       --val-size 256
 */
public class BenchmarkClient {

    static int numClients   = 200;
    static int targetRate   = 1000;
    static int durationSecs = 10;
    static int keySize      = 8;
    static int valSize      = 256;
    static String configFile = "paxos.properties";

    static class Sample {
        final long sendMs;
        final long latencyNs;
        Sample(long sendMs, long latencyNs) {
            this.sendMs    = sendMs;
            this.latencyNs = latencyNs;
        }
    }

    public static void main(String[] args) throws Exception {
        parseArgs(args);

        System.err.printf("Open-loop benchmark: rate=%d rps  clients=%d  duration=%ds%n",
                targetRate, numClients, durationSecs);

        Configuration config = new Configuration(configFile);

        // Pre-connect all clients
        BlockingQueue<Client> pool = new ArrayBlockingQueue<>(numClients);
        for (int i = 0; i < numClients; i++) {
            Client c = new Client(config);
            c.connect();
            pool.add(c);
        }

        ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
        AtomicLong dropped = new AtomicLong(0);
        AtomicLong errors  = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(numClients);

        byte[] payload = new byte[keySize + valSize];
        new Random().nextBytes(payload);

        long intervalNs  = 1_000_000_000L / targetRate;
        long nextFireNs  = System.nanoTime();
        long benchStartMs = System.currentTimeMillis();
        long endMs        = benchStartMs + (long) durationSecs * 1000;

        // Scheduler loop: fire at exactly targetRate/s, fire-and-forget.
        while (true) {
            long nowNs = System.nanoTime();
            if (nowNs < nextFireNs) {
                LockSupport.parkNanos(nextFireNs - nowNs);
                continue;
            }
            if (System.currentTimeMillis() >= endMs) break;

            nextFireNs += intervalNs;

            // Non-blocking poll: if no idle client, count as dropped.
            final Client client = pool.poll();
            if (client == null) {
                dropped.incrementAndGet();
                continue;
            }

            final long sendMs = System.currentTimeMillis();
            final long sendNs = System.nanoTime();

            executor.submit(() -> {
                try {
                    client.execute(payload);
                    samples.add(new Sample(sendMs, System.nanoTime() - sendNs));
                } catch (ReplicationException e) {
                    errors.incrementAndGet();
                } finally {
                    pool.offer(client);
                }
            });
        }

        long benchEndMs = System.currentTimeMillis();

        // Give in-flight requests up to 5 s to drain before printing results.
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

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
                case "--clients":  numClients   = Integer.parseInt(args[++i]); break;
                case "--key-size": keySize      = Integer.parseInt(args[++i]); break;
                case "--val-size": valSize      = Integer.parseInt(args[++i]); break;
                default:
                    System.err.printf("Unknown option: %s%n", args[i]);
            }
        }
    }
}
