package com.usatiuk.dhfsfs.benchmarks;

import io.quarkus.logging.Log;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;
import java.util.function.Supplier;

public class Benchmarker {
    static <T> long[] runLatency(Supplier<T> fn, int iterations) {
        var out = new long[iterations];

        int hash = 1;

        for (int i = 0; i < iterations; i++) {
            long startNanos = System.nanoTime();
            var cur = fn.get();
            long stopNanos = System.nanoTime();
            out[i] = stopNanos - startNanos;
            hash = hash * 31 + cur.hashCode();
        }

        System.out.println("\nHash: " + hash);

        return out;
    }

    static <T> long[] runThroughput(Supplier<T> fn, int iterations, long iterationTime) {
        var out = new long[iterations];

        int hash = 1;

        for (int i = 0; i < iterations; i++) {
            long startMillis = System.currentTimeMillis();
            long count = 0;
            // FIXME: That's probably janky
            while (System.currentTimeMillis() - startMillis < iterationTime) {
                var res = fn.get();
                count++;
                hash = hash * 31 + res.hashCode();
            }
            System.out.println("Ran iteration " + i + "/" + iterations + " count=" + count);
            out[i] = count;
        }

        System.out.println("\nHash: " + hash);

        return out;
    }

    static void printStats(double[] data, String unit) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (var r : data) {
            stats.addValue(r);
        }
        Log.info("\n" + stats +
                "\n 50%: " + stats.getPercentile(50) + " " + unit +
                "\n 90%: " + stats.getPercentile(90) + " " + unit +
                "\n 95%: " + stats.getPercentile(95) + " " + unit +
                "\n 99%: " + stats.getPercentile(99) + " " + unit +
                "\n 99.9%: " + stats.getPercentile(99.9) + " " + unit +
                "\n 99.99%: " + stats.getPercentile(99.99) + " " + unit
        );

    }

    static <T> void runAndPrintMixSimple(String name, Supplier<T> fn, int latencyIterations, int thrptIterations, int thrptIterationTime, int warmupIterations, int warmupIterationTime) {
        System.out.println("\n=========\n" + "Running " + name + "\n=========\n");
        System.out.println("==Warmup==");
        runThroughput(fn, warmupIterations, warmupIterationTime);
        System.out.println("==Warmup done==");
        System.out.println("==Throughput==");
        var thrpt = runThroughput(fn, thrptIterations, thrptIterationTime);
        printStats(Arrays.stream(thrpt).mapToDouble(o -> (double) o / 1000).toArray(), "ops/s");
        System.out.println("==Throughput done==");
        System.out.println("==Latency==");
        var lat = runLatency(fn, latencyIterations);
        printStats(Arrays.stream(lat).mapToDouble(o -> (double) o).toArray(), "ns/op");
        System.out.println("==Latency done==");
        System.out.println("\n=========\n" + name + " done" + "\n=========\n");
    }

}
