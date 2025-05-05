package com.usatiuk.dhfsfuse.integration;

import io.quarkus.logging.Log;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LazyFs {
    private static final String lazyFsPath;

    static {
        lazyFsPath = System.getProperty("lazyFsPath");
        System.out.println("LazyFs Path: " + lazyFsPath);
    }

    private final String mountRoot;
    private final String dataRoot;
    private final String name;
    private final File configFile;
    private final File fifoFile;
    private Thread errPiper;
    private Thread outPiper;
    private CountDownLatch startLatch;
    private Process fs;
    public LazyFs(String name, String mountRoot, String dataRoot) {
        this.name = name;
        this.mountRoot = mountRoot;
        this.dataRoot = dataRoot;

        try {
            configFile = File.createTempFile("lazyfs", ".conf");
            configFile.deleteOnExit();

            fifoFile = new File("/tmp/" + ThreadLocalRandom.current().nextLong() + ".faultsfifo");
            fifoFile.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private String fifoPath() {
        return fifoFile.getAbsolutePath();
    }

    public void start(String extraOpts) {
        var lfsPath = Path.of(lazyFsPath).resolve("build").resolve("lazyfs");
        if (!lfsPath.toFile().isFile())
            throw new IllegalStateException("LazyFs binary does not exist: " + lfsPath.toAbsolutePath());
        if (!lfsPath.toFile().canExecute())
            throw new IllegalStateException("LazyFs binary is not executable: " + lfsPath.toAbsolutePath());

        try (var rwFile = new RandomAccessFile(configFile, "rw");
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
                    "logfile=\"\"\n" + extraOpts;
            rwFile.write(config.getBytes());
            Log.info("LazyFs config: \n" + config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        var argList = new ArrayList<String>();

        argList.add(lfsPath.toString());
        argList.add(Path.of(mountRoot).toString());
        argList.add("--config-path");
        argList.add(configFile.getAbsolutePath());
        argList.add("-o");
        argList.add("allow_other");
        argList.add("-o");
        argList.add("modules=subdir");
        argList.add("-o");
        argList.add("subdir=" + Path.of(dataRoot).toAbsolutePath().toString());
        try {
            Log.info("Starting LazyFs " + argList);
            fs = Runtime.getRuntime().exec(argList.toArray(String[]::new));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        startLatch = new CountDownLatch(1);

        outPiper = new Thread(() -> {
            try {
                try (BufferedReader input = new BufferedReader(new InputStreamReader(fs.getInputStream()))) {
                    String line;

                    while ((line = input.readLine()) != null) {
                        if (line.contains("running LazyFS"))
                            startLatch.countDown();
                        System.out.println(line);
                    }
                }
            } catch (Exception e) {
                Log.info("Exception in LazyFs piper", e);
            }
            Log.info("LazyFs out piper finished");
        });
        outPiper.start();
        errPiper = new Thread(() -> {
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
            Log.info("LazyFs err piper finished");
        });
        errPiper.start();

        try {
            if (!startLatch.await(30, TimeUnit.SECONDS))
                throw new RuntimeException("StartLatch timed out");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Log.info("LazyFs started");
    }

    public void start() {
        start("");
    }

    private String mdbPath() {
        return Path.of(dataRoot).resolve("objects").resolve("data.mdb").toAbsolutePath().toString();
    }

    public void startTornOp() {
        start("\n" +
                "[[injection]]\n" +
                "type=\"torn-seq\"\n" +
                "op=\"write\"\n" +
                "file=\"" + mdbPath() + "\"\n" +
                "persist=[1,4]\n" +
                "occurrence=3");
    }

    public void startTornSeq() {
        start("[[injection]]\n" +
                "type=\"torn-op\"\n" +
                "file=\"" + mdbPath() + "\"\n" +
                "occurrence=3\n" +
                "parts=3 #or parts_bytes=[4096,3600,1260]\n" +
                "persist=[1,3]");
    }

    public void crash() {
        try {
            var cmd = "echo \"lazyfs::crash::timing=after::op=write::from_rgx=*\" > " + fifoPath();
            Log.info("Running command: " + cmd);
            Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd}).waitFor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            synchronized (this) {
                Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "fusermount3 -u " + mountRoot}).waitFor();
            }
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

