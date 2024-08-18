package com.usatiuk.dhfs.benchmarks;

import io.quarkus.logging.Log;

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
            Log.info("Ran iteration " + i + "/" + iterations + " count=" + count);
            out[i] = count;
        }

        System.out.println("\nHash: " + hash);

        return out;
    }

}
