package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import com.usatiuk.dhfs.TestDataCleaner;
import io.quarkus.logging.Log;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

public class LazyFsIT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;

    String c1uuid;
    String c2uuid;

    File data1;
    File data2;
    File data1Lazy;
    File data2Lazy;

    LazyFs lazyFs1;
    LazyFs lazyFs2;

    ExecutorService executor;
    Network network;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        executor = Executors.newCachedThreadPool();
        data1 = Files.createTempDirectory("dhfsdata").toFile();
        data2 = Files.createTempDirectory("dhfsdata").toFile();
        data1Lazy = Files.createTempDirectory("lazyfsroot").toFile();
        data2Lazy = Files.createTempDirectory("lazyfsroot").toFile();

        network = Network.newNetwork();

        lazyFs1 = new LazyFs(testInfo.getDisplayName(), data1.toString(), data1Lazy.toString());
        lazyFs1.start();
        lazyFs2 = new LazyFs(testInfo.getDisplayName(), data2.toString(), data2Lazy.toString());
        lazyFs2.start();

        container1 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network)
                .withFileSystemBind(data1.getAbsolutePath(), "/dhfs_test/data");
        container2 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network)
                .withFileSystemBind(data2.getAbsolutePath(), "/dhfs_test/data");

        Stream.of(container1, container2).parallel().forEach(GenericContainer::start);

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/data/stuff/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/data/stuff/self_uuid").getStdout();

        Assertions.assertDoesNotThrow(() -> UUID.fromString(c1uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c2uuid));

        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("New address"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("New address"), 60, TimeUnit.SECONDS);

        var c1curl = container1.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        var c2curl = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
    }

    @AfterEach
    void stop() {
        lazyFs1.stop();
        lazyFs2.stop();

        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
        TestDataCleaner.purgeDirectory(data1);
        TestDataCleaner.purgeDirectory(data1Lazy);
        TestDataCleaner.purgeDirectory(data2);
        TestDataCleaner.purgeDirectory(data2Lazy);

        executor.close();
        network.close();
    }

    private void checkConsistency(String testName) {
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            Log.info("Listing consistency " + testName + "\n"
                    + ls1 + "\n"
                    + cat1 + "\n"
                    + ls2 + "\n"
                    + cat2 + "\n");

            return ls1.equals(ls2) && cat1.equals(cat2);
        });
    }

    private static enum CrashType {
        CRASH,
        TORN_OP,
        TORN_SEQ
    }

    @ParameterizedTest
    @EnumSource(CrashType.class)
    void killTest(CrashType crashType, TestInfo testInfo) throws Exception {
        var barrier = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test1; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Thread.sleep(3000);
        Log.info("Killing");
        lazyFs1.crash();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        switch (crashType) {
            case CRASH -> lazyFs1.start();
            case TORN_OP -> lazyFs1.startTornOp();
            case TORN_SEQ -> lazyFs1.startTornSeq();
        }
        container1.start();

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test2; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Log.info("Killing");
        if (crashType.equals(CrashType.CRASH)) {
            Thread.sleep(3000);
            lazyFs1.crash();
        }
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        lazyFs1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        lazyFs1.start();
        container1.start();

        waitingConsumer1 = new WaitingConsumer();
        loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        checkConsistency(testInfo.getDisplayName());
    }


    @ParameterizedTest
    @EnumSource(CrashType.class)
    void killTestDirs(CrashType crashType, TestInfo testInfo) throws Exception {
        var barrier = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test$counter; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Thread.sleep(3000);
        Log.info("Killing");
        lazyFs1.crash();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        switch (crashType) {
            case CRASH -> lazyFs1.start();
            case TORN_OP -> lazyFs1.startTornOp();
            case TORN_SEQ -> lazyFs1.startTornSeq();
        }
        container1.start();

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/2test$counter; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Log.info("Killing");
        if (crashType.equals(CrashType.CRASH)) {
            Thread.sleep(3000);
            lazyFs1.crash();
        }
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        lazyFs1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        lazyFs1.start();
        container1.start();

        waitingConsumer1 = new WaitingConsumer();
        loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        checkConsistency(testInfo.getDisplayName());
    }

    @ParameterizedTest
    @EnumSource(CrashType.class)
    void killTest2(CrashType crashType, TestInfo testInfo) throws Exception {
        var barrier = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while  [ ! -f /tmp/stopprinting1 ]; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test1; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Log.info("Killing");
        lazyFs2.crash();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container2.getContainerId()).exec();
        container2.stop();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        switch (crashType) {
            case CRASH -> lazyFs2.start();
            case TORN_OP -> lazyFs2.startTornOp();
            case TORN_SEQ -> lazyFs2.startTornSeq();
        }
        container1.execInContainer("/bin/sh", "-c", "touch /tmp/stopprinting1");
        container2.start();

        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        var barrier2 = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier2.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while  [ ! -f /tmp/stopprinting2 ]; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test2; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier2.await();
        Log.info("Killing");
        if (crashType.equals(CrashType.CRASH)) {
            Thread.sleep(2000);
            lazyFs2.crash();
        }
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 30, TimeUnit.SECONDS);
        client.killContainerCmd(container2.getContainerId()).exec();
        container2.stop();
        lazyFs2.stop();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        container1.execInContainer("/bin/sh", "-c", "touch /tmp/stopprinting2");
        lazyFs2.start();
        container2.start();

        waitingConsumer2 = new WaitingConsumer();
        loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        checkConsistency(testInfo.getDisplayName());
    }


    @ParameterizedTest
    @EnumSource(CrashType.class)
    void killTestDirs2(CrashType crashType, TestInfo testInfo) throws Exception {
        var barrier = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while  [ ! -f /tmp/stopprinting1 ]; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/test$counter; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Thread.sleep(3000);
        Log.info("Killing");
        lazyFs2.crash();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 5, TimeUnit.SECONDS);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container2.getContainerId()).exec();
        container2.stop();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Log.info("Restart");
        switch (crashType) {
            case CRASH -> lazyFs2.start();
            case TORN_OP -> lazyFs2.startTornOp();
            case TORN_SEQ -> lazyFs2.startTornSeq();
        }
        container1.execInContainer("/bin/sh", "-c", "touch /tmp/stopprinting1");
        container2.start();

        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        var barrier2 = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier2.countDown();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while  [ ! -f /tmp/stopprinting2 ]; do counter=`expr $counter + 1`; echo $counter >> /dhfs_test/fuse/2test$counter; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier2.await();
        Log.info("Killing");
        if (crashType.equals(CrashType.CRASH)) {
            Thread.sleep(2000);
            lazyFs2.crash();
        }
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Caused by: org.lmdbjava"), 30, TimeUnit.SECONDS);
        client.killContainerCmd(container2.getContainerId()).exec();
        container2.stop();
        lazyFs2.stop();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        container1.execInContainer("/bin/sh", "-c", "touch /tmp/stopprinting2");
        Log.info("Restart");
        lazyFs2.start();
        container2.start();

        waitingConsumer2 = new WaitingConsumer();
        loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(LazyFsIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        checkConsistency(testInfo.getDisplayName());
    }


}
