package com.usatiuk.dhfs.integration;

import io.quarkus.logging.Log;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LazyFs {
    private static final String lazyFsPath;

    private final String dataRoot;
    private final String lazyFsDataPath;
    private final String name;
    private final File tmpConfigFile;
    private final File fifoPath;

    static {
        lazyFsPath = System.getProperty("lazyFsPath");
        System.out.println("LazyFs Path: " + lazyFsPath);
    }

    public LazyFs(String name, String dataRoot, String lazyFsDataPath) {
        this.name = name;
        this.dataRoot = dataRoot;
        this.lazyFsDataPath = lazyFsDataPath;

        try {
            tmpConfigFile = File.createTempFile("lazyfs", ".conf");
            tmpConfigFile.deleteOnExit();

            fifoPath = new File("/tmp/" + ThreadLocalRandom.current().nextLong() + ".faultsfifo");
            fifoPath.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Thread outputPiper;
    private Process fs;

    private String fifoPath() {
        return fifoPath.getAbsolutePath();
    }

    public void start() {
        var lfsPath = Path.of(lazyFsPath).resolve("build").resolve("lazyfs");
        if (!lfsPath.toFile().isFile())
            throw new IllegalStateException("LazyFs binary does not exist: " + lfsPath.toAbsolutePath());
        if (!lfsPath.toFile().canExecute())
            throw new IllegalStateException("LazyFs binary is not executable: " + lfsPath.toAbsolutePath());

        try (var rwFile = new RandomAccessFile(tmpConfigFile, "rw");
             var channel = rwFile.getChannel()) {
            channel.truncate(0);
            var config = "[faults]\n" +
                    "fifo_path=\"" + fifoPath() + "\"\n" +
                    "[cache]\n" +
                    "apply_eviction=false\n" +
                    "[cache.simple]\n" +
                    "custom_size=\"1gb\"\n" +
                    "blocks_per_page=1\n" +
                    "[filesystem]\n" +
                    "log_all_operations=false\n" +
                    "logfile=\"\"";
            rwFile.write(config.getBytes());
            Log.info("LazyFs config: \n" + config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        var argList = new ArrayList<String>();

        argList.add(lfsPath.toString());
        argList.add(Path.of(dataRoot).toString());
        argList.add("--config-path");
        argList.add(tmpConfigFile.getAbsolutePath());
        argList.add("-o");
        argList.add("allow_other");
        argList.add("-o");
        argList.add("modules=subdir");
        argList.add("-o");
        argList.add("subdir=" + Path.of(lazyFsDataPath).toAbsolutePath().toString());
        try {
            Log.info("Starting LazyFs " + argList);
            fs = Runtime.getRuntime().exec(argList.toArray(String[]::new));
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        outputPiper = new Thread(() -> {
            try {
                try (BufferedReader input = new BufferedReader(new InputStreamReader(fs.getInputStream()))) {
                    String line;

                    while ((line = input.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } catch (Exception e) {
                Log.info("Exception in LazyFs piper", e);
            }
        });
        outputPiper.start();
        outputPiper = new Thread(() -> {
            try {
                try (BufferedReader input = new BufferedReader(new InputStreamReader(fs.getErrorStream()))) {
                    String line;

                    while ((line = input.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } catch (Exception e) {
                Log.info("Exception in LazyFs piper", e);
            }
        });
        outputPiper.start();
    }

    public void crash() {
        try {
            var cmd = "echo \"lazyfs::crash::timing=after::op=write::from_rgx=*\" > " + fifoPath();
            System.out.println("Running command: " + cmd);
            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd});
            stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            if (fs == null) {
                return;
            }
            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "fusermount3 -u " + dataRoot});
            if (!fs.waitFor(1, TimeUnit.SECONDS))
                throw new RuntimeException("LazyFs process did not stop in time");
            fs = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    Doesn't actually work?
//
//    public void crashop() {
//        try {
//            var cmd = "echo \"lazyfs::torn-op::file=" + Path.of(lazyFsDataPath).toAbsolutePath().toString() + "/objects/data.mdb::persist=1,3::parts=3::occurrence=5\" > /tmp/faults.fifo";
//            System.out.println("Running command: " + cmd);
//            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd});
//            Thread.sleep(1000);
//            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "fusermount3 -u " + dataRoot});
//            Thread.sleep(1000);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public void crashseq() {
//        try {
//            var cmd = "echo \"lazyfs::torn-seq::op=write::file=" + Path.of(lazyFsDataPath).toAbsolutePath().toString() + "/objects/data.mdb::persist=1,4::occurrence=2\" > /tmp/faults.fifo";
//            System.out.println("Running command: " + cmd);
//            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd});
//            Thread.sleep(1000);
//            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "fusermount3 -u " + dataRoot});
//            Thread.sleep(1000);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
}

