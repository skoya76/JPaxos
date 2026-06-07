package lsr.paxos.test.etcdstyle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Smoke-test for {@link EtcdStyleBenchmarkClient} using an in-process fake
 * JPaxos server that speaks the same wire protocol as a real replica.
 */
public final class EtcdStyleBenchmarkClientSelfTest {
    private EtcdStyleBenchmarkClientSelfTest() {
    }

    public static void main(String[] args) throws Exception {
        runBasic();
        runTotalOverride();
        runRequestTimeout();
        runDrainTimeoutSafetyNet();
        System.out.println("EtcdStyleBenchmarkClientSelfTest: OK");
    }

    private static void runBasic() throws Exception {
        FakeServer server = new FakeServer();
        Thread serverThread = new Thread(server, "etcd-style-fake-server");
        serverThread.setDaemon(true);
        serverThread.start();

        File config = File.createTempFile("etcd-style-selftest", ".properties");
        try (FileWriter writer = new FileWriter(config)) {
            writer.write("process.0 = 127.0.0.1:1:" + server.port() + "\n");
        }

        int rate = 200;
        int duration = 1;
        EtcdStyleBenchmarkClient.main(new String[] {
                "--config", config.getAbsolutePath(),
                "--rate", Integer.toString(rate),
                "--duration", Integer.toString(duration),
                "--clients", "4",
                "--connections", "2",
                "--key-size", "4",
                "--val-size", "4"
        });

        server.stop();
        serverThread.join(1000);
        // Total = rate * duration; allow small slack for paced limiter rounding.
        int got = server.requests.get();
        assertTrue(got >= rate * duration - 5 && got <= rate * duration,
                "fake server requests: expected~" + (rate * duration) + " got=" + got);
    }

    private static void runTotalOverride() throws Exception {
        FakeServer server = new FakeServer();
        Thread serverThread = new Thread(server, "etcd-style-fake-server-total");
        serverThread.setDaemon(true);
        serverThread.start();

        File config = File.createTempFile("etcd-style-selftest-total", ".properties");
        try (FileWriter writer = new FileWriter(config)) {
            writer.write("process.0 = 127.0.0.1:1:" + server.port() + "\n");
        }

        int total = 50;
        EtcdStyleBenchmarkClient.main(new String[] {
                "--config", config.getAbsolutePath(),
                "--rate", "200",
                "--duration", "1",
                "--total", Integer.toString(total),
                "--clients", "2",
                "--connections", "1",
                "--key-size", "4",
                "--val-size", "4"
        });

        server.stop();
        serverThread.join(1000);
        assertEquals(total, server.requests.get(), "fake server total requests");
    }

    private static void runRequestTimeout() throws Exception {
        // Server accepts the connection and assigns a client id, then never replies.
        // With --request-timeout-ms set, the benchmark must finish promptly with
        // Timeouts == total instead of hanging forever.
        SilentFakeServer server = new SilentFakeServer();
        Thread serverThread = new Thread(server, "etcd-style-silent-server");
        serverThread.setDaemon(true);
        serverThread.start();

        File config = File.createTempFile("etcd-style-selftest-timeout", ".properties");
        try (FileWriter writer = new FileWriter(config)) {
            writer.write("process.0 = 127.0.0.1:1:" + server.port() + "\n");
        }

        long t0 = System.currentTimeMillis();
        String output = captureStdout(() -> EtcdStyleBenchmarkClient.main(new String[] {
                    "--config", config.getAbsolutePath(),
                    "--rate", "100",
                    "--duration", "1",
                    "--total", "5",
                    "--clients", "5",
                    "--connections", "1",
                    "--request-timeout-ms", "500",
                    "--drain-timeout", "2",
                    "--key-size", "4",
                    "--val-size", "4"
        }));
        long elapsed = System.currentTimeMillis() - t0;

        server.stop();
        serverThread.join(1000);

        // 5 workers, each fires once, each times out after ~500ms.
        // Should finish well under 10s even with worst-case scheduling.
        assertTrue(elapsed < 10_000, "timeout test should finish under 10s, got=" + elapsed + "ms");
        assertTrue(output.contains("  Timeouts:\t5"), "summary should report all request timeouts");
        assertTrue(hasPerSecondTimeout(output), "per-second samples should include timeout outcomes");
    }

    private static void runDrainTimeoutSafetyNet() throws Exception {
        // No per-request timeout, server never replies, but --drain-timeout
        // should still bound the total wait via worker interruption.
        SilentFakeServer server = new SilentFakeServer();
        Thread serverThread = new Thread(server, "etcd-style-silent-server-drain");
        serverThread.setDaemon(true);
        serverThread.start();

        File config = File.createTempFile("etcd-style-selftest-drain", ".properties");
        try (FileWriter writer = new FileWriter(config)) {
            writer.write("process.0 = 127.0.0.1:1:" + server.port() + "\n");
        }

        long t0 = System.currentTimeMillis();
        EtcdStyleBenchmarkClient.main(new String[] {
                "--config", config.getAbsolutePath(),
                "--rate", "10",
                "--duration", "1",
                "--total", "1",
                "--clients", "1",
                "--connections", "1",
                "--request-timeout-ms", "0",
                "--drain-timeout", "2",
                "--key-size", "4",
                "--val-size", "4"
        });
        long elapsed = System.currentTimeMillis() - t0;

        server.stop();
        serverThread.join(1000);

        // estimatedRunSecs = 1/10 = 0.1, so maxRunBudgetMs is clamped to 60s,
        // plus 2s drain = ~62s upper bound. We mostly care that it returns.
        assertTrue(elapsed < 70_000, "drain safety net should fire within ~62s, got=" + elapsed + "ms");
    }

    private static final class SilentFakeServer implements Runnable {
        final ServerSocket serverSocket;
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger nextClientId = new AtomicInteger(5000);

        SilentFakeServer() throws Exception {
            serverSocket = new ServerSocket(0);
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void stop() throws Exception {
            running.set(false);
            serverSocket.close();
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    Socket socket = serverSocket.accept();
                    Thread h = new Thread(() -> handle(socket), "etcd-style-silent-client");
                    h.setDaemon(true);
                    h.start();
                } catch (Exception e) {
                    if (running.get()) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void handle(Socket socket) {
            try (Socket s = socket;
                 DataInputStream input = new DataInputStream(s.getInputStream());
                 DataOutputStream output = new DataOutputStream(s.getOutputStream())) {
                int init = input.readUnsignedByte();
                if (init != EtcdStyleProtocolCodec.REQUEST_NEW_ID) {
                    return;
                }
                output.writeLong(nextClientId.getAndIncrement());
                output.flush();
                // Drain incoming requests but never send replies.
                while (running.get()) {
                    input.readInt();   // command type
                    input.readLong();  // client id
                    input.readInt();   // sequence id
                    int valueLength = input.readInt();
                    byte[] value = new byte[valueLength];
                    input.readFully(value);
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static final class FakeServer implements Runnable {
        final ServerSocket serverSocket;
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger nextClientId = new AtomicInteger(2000);
        final AtomicInteger requests = new AtomicInteger();

        FakeServer() throws Exception {
            serverSocket = new ServerSocket(0);
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void stop() throws Exception {
            running.set(false);
            serverSocket.close();
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    Socket socket = serverSocket.accept();
                    Thread handler = new Thread(() -> handle(socket), "etcd-style-fake-client");
                    handler.setDaemon(true);
                    handler.start();
                } catch (Exception e) {
                    if (running.get()) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void handle(Socket socket) {
            try (Socket s = socket;
                 DataInputStream input = new DataInputStream(s.getInputStream());
                 DataOutputStream output = new DataOutputStream(s.getOutputStream())) {
                int init = input.readUnsignedByte();
                if (init != EtcdStyleProtocolCodec.REQUEST_NEW_ID) {
                    throw new AssertionError("unexpected init byte: " + init);
                }
                output.writeLong(nextClientId.getAndIncrement());
                output.flush();

                while (running.get()) {
                    int commandType = input.readInt();
                    long clientId = input.readLong();
                    int sequenceId = input.readInt();
                    int valueLength = input.readInt();
                    byte[] value = new byte[valueLength];
                    input.readFully(value);
                    if (commandType != EtcdStyleProtocolCodec.COMMAND_REQUEST) {
                        throw new AssertionError("unexpected command type: " + commandType);
                    }
                    requests.incrementAndGet();
                    ByteBuffer reply = EtcdStyleProtocolCodec.encodeOkClientReply(clientId, sequenceId, value);
                    synchronized (output) {
                        output.write(reply.array(), reply.position(), reply.remaining());
                        output.flush();
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static void assertEquals(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected=" + expected + " actual=" + actual);
        }
    }

    private static void assertTrue(boolean condition, String label) {
        if (!condition) {
            throw new AssertionError(label);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static String captureStdout(ThrowingRunnable runnable) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream replacement = new PrintStream(buffer, true, StandardCharsets.UTF_8.name())) {
            System.setOut(replacement);
            runnable.run();
        } finally {
            System.setOut(original);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static boolean hasPerSecondTimeout(String output) {
        for (String line : output.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || !Character.isDigit(trimmed.charAt(0))) {
                continue;
            }
            String[] fields = trimmed.split(",");
            if (fields.length >= 10 && Integer.parseInt(fields[8]) > 0) {
                return true;
            }
        }
        return false;
    }
}
