package lsr.paxos.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BenchmarkClientTest {

    private PrintStream origOut;
    private ByteArrayOutputStream captured;

    @Before
    public void captureStdout() {
        origOut   = System.out;
        captured  = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured));
    }

    @After
    public void restoreStdout() {
        System.setOut(origOut);
    }

    private String output() {
        return captured.toString();
    }

    // -----------------------------------------------------------------------
    // percentileIndex
    // -----------------------------------------------------------------------

    @Test
    public void testPercentileIndexBoundary() {
        // p=100% with count=1 must return index 0, not -1
        assertEquals(0, BenchmarkClient.percentileIndex(1, 1.0));
        assertEquals(0, BenchmarkClient.percentileIndex(1, 0.0));
    }

    @Test
    public void testPercentileIndexMedian() {
        // 50th percentile of 10 elements → ceil(0.5*10)-1 = 4
        assertEquals(4, BenchmarkClient.percentileIndex(10, 0.50));
    }

    @Test
    public void testPercentileIndex99() {
        // 99th of 100 → ceil(0.99*100)-1 = 98
        assertEquals(98, BenchmarkClient.percentileIndex(100, 0.99));
    }

    // -----------------------------------------------------------------------
    // printResults output format (verified against analyze_benchmark_results.py regexes)
    // -----------------------------------------------------------------------

    private List<BenchmarkClient.Sample> makeSamples(long baseMs, int count,
                                                       long minNs, long maxNs) {
        List<BenchmarkClient.Sample> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long latNs = minNs + (maxNs - minNs) * i / Math.max(count - 1, 1);
            // spread sends over 3 seconds so per-second section has >=3 entries
            list.add(new BenchmarkClient.Sample(baseMs + i * 3000L / count, latNs));
        }
        return list;
    }

    @Test
    public void testSummarySection() {
        List<BenchmarkClient.Sample> samples = makeSamples(0L, 100, 1_000_000L, 10_000_000L);
        BenchmarkClient.printResults(samples, 0L, 3000L, 0L, 0L);
        String out = output();

        assertTrue("Missing Summary:", out.contains("Summary:"));
        assertTrue("Missing Total:",        out.contains("Total:"));
        assertTrue("Missing Slowest:",      out.contains("Slowest:"));
        assertTrue("Missing Fastest:",      out.contains("Fastest:"));
        assertTrue("Missing Average:",      out.contains("Average:"));
        assertTrue("Missing Stddev:",       out.contains("Stddev:"));
        assertTrue("Missing Requests/sec:", out.contains("Requests/sec:"));
    }

    @Test
    public void testSummarySectionRegex() {
        List<BenchmarkClient.Sample> samples = makeSamples(0L, 100, 1_000_000L, 10_000_000L);
        BenchmarkClient.printResults(samples, 0L, 3000L, 0L, 0L);
        String out = output();

        // Same regexes as analyze_benchmark_results.py
        assertTrue(Pattern.compile(r("Total:\\s*([0-9.]+)\\s*secs")).matcher(out).find());
        assertTrue(Pattern.compile(r("Slowest:\\s*([0-9.]+)\\s*secs")).matcher(out).find());
        assertTrue(Pattern.compile(r("Fastest:\\s*([0-9.]+)\\s*secs")).matcher(out).find());
        assertTrue(Pattern.compile(r("Average:\\s*([0-9.]+)\\s*secs")).matcher(out).find());
        assertTrue(Pattern.compile(r("Stddev:\\s*([0-9.]+)\\s*secs")).matcher(out).find());
        assertTrue(Pattern.compile(r("Requests/sec:\\s*([0-9.]+)")).matcher(out).find());
    }

    @Test
    public void testHistogramSection() {
        List<BenchmarkClient.Sample> samples = makeSamples(0L, 100, 1_000_000L, 10_000_000L);
        BenchmarkClient.printResults(samples, 0L, 3000L, 0L, 0L);
        String out = output();

        assertTrue(out.contains("Response time histogram:"));
        // Pattern from analyze_benchmark_results.py: ([0-9.]+)\s+\[(\d+)\]
        assertTrue(Pattern.compile(r("([0-9.]+)\\s+\\[(\\d+)\\]")).matcher(out).find());
    }

    @Test
    public void testLatencyDistributionSection() {
        List<BenchmarkClient.Sample> samples = makeSamples(0L, 1000, 1_000_000L, 100_000_000L);
        BenchmarkClient.printResults(samples, 0L, 5000L, 0L, 0L);
        String out = output();

        assertTrue(out.contains("Latency distribution:"));
        // Pattern from analyze_benchmark_results.py: (\d+(?:\.\d+)?)%\s+in\s+([0-9.]+)\s+secs
        Pattern p = Pattern.compile(r("(\\d+(?:\\.\\d+)?)%\\s+in\\s+([0-9.]+)\\s+secs"));
        Matcher m = p.matcher(out);
        int matchCount = 0;
        while (m.find()) matchCount++;
        assertEquals("Expected 6 percentile lines (10,50,90,95,99,99.9)", 6, matchCount);
    }

    @Test
    public void testSampleInOneSecondSection() {
        long baseMs = System.currentTimeMillis();
        List<BenchmarkClient.Sample> samples = makeSamples(baseMs, 300, 1_000_000L, 5_000_000L);
        BenchmarkClient.printResults(samples, baseMs, baseMs + 3000L, 0L, 0L);
        String out = output();

        assertTrue(out.contains("Sample in one second:"));
        assertTrue(out.contains("UNIX-SECOND,min_latency_ms,avg_latency_ms,max_latency_ms,avg_throughput"));

        // Each data line: unix_second (int), 3 float columns, count (int)
        Pattern p = Pattern.compile(r("^\\s*(\\d+),([0-9.]+),([0-9.]+),([0-9.]+),(\\d+)"),
                                    Pattern.MULTILINE);
        assertTrue("Expected at least one sample data line", p.matcher(out).find());
    }

    @Test
    public void testBlankLinesBetweenSections() {
        List<BenchmarkClient.Sample> samples = makeSamples(0L, 100, 1_000_000L, 10_000_000L);
        BenchmarkClient.printResults(samples, 0L, 3000L, 0L, 0L);
        String out = output();

        // analyze_benchmark_results.py relies on \n\n to delimit sections
        assertTrue("Summary must be followed by blank line",
            out.contains("Requests/sec:") && out.contains("\n\n"));
        // Check that at least 3 double-newlines exist (between 4 sections)
        int count = 0;
        int idx   = 0;
        while ((idx = out.indexOf("\n\n", idx)) != -1) { count++; idx += 2; }
        assertTrue("Expected >=3 blank lines separating sections, got " + count, count >= 3);
    }

    @Test
    public void testAllSameSingleLatency() {
        // Edge case: all requests have identical latency
        List<BenchmarkClient.Sample> samples = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            samples.add(new BenchmarkClient.Sample(1000L * i, 5_000_000L));
        }
        BenchmarkClient.printResults(samples, 0L, 10000L, 0L, 0L);
        String out = output();

        assertTrue(out.contains("Summary:"));
        // Fastest == Slowest when all latencies are equal
        Pattern fastest = Pattern.compile(r("Fastest:\\s*([0-9.]+)\\s*secs"));
        Pattern slowest = Pattern.compile(r("Slowest:\\s*([0-9.]+)\\s*secs"));
        Matcher mf = fastest.matcher(out);
        Matcher ms = slowest.matcher(out);
        assertTrue(mf.find()); assertTrue(ms.find());
        assertEquals(Double.parseDouble(mf.group(1)),
                     Double.parseDouble(ms.group(1)), 1e-9);
    }

    // Small helper so test code doesn't need double-escaping everywhere
    private static String r(String pattern) { return pattern; }
}
