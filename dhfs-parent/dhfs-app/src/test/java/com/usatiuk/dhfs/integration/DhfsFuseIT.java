package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

public class DhfsFuseIT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;

    String c1uuid;
    String c2uuid;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        Network network = Network.newNetwork();
        container1 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);
        container2 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);

        Stream.of(container1, container2).parallel().forEach(GenericContainer::start);

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFuseIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFuseIT.class)).withPrefix("2-" + testInfo.getDisplayName());
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
        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }

    @Test
    void readWriteRewriteFileTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo rewritten > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "rewritten\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }

    @Test
    void createDelayedTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo newfile > /dhfs_test/fuse/testf2").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "newfile\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf2").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "newfile\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf2").getStdout()));
    }

    @Test
    void writeRewriteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo rewritten > /dhfs_test/fuse/testf1").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "rewritten\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "rewritten\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }

    // TODO: How this fits with the tree?
    @Test
    @Disabled
    void deleteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "rm /dhfs_test/fuse/testf1").getExitCode());
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Delaying deletion check"), 60, TimeUnit.SECONDS, 1);

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getExitCode());

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 1);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 1 == container2.execInContainer("/bin/sh", "-c", "test -f /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 1 == container1.execInContainer("/bin/sh", "-c", "test -f /dhfs_test/fuse/testf1").getExitCode());
    }

    @Test
    void deleteTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        Log.info("Deleting");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "rm /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getExitCode());
        Log.info("Deleted");

        // FIXME?
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        await().atMost(45, TimeUnit.SECONDS).until(() ->
                1 == container2.execInContainer("/bin/sh", "-c", "test -f /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() ->
                1 == container1.execInContainer("/bin/sh", "-c", "test -f /dhfs_test/fuse/testf1").getExitCode());
    }

    @Test
    void moveFileTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        Log.info("Moving");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/testf1 /dhfs_test/fuse/testf2").getExitCode());
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        Log.info("Reading");
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf2").getStdout()));
    }

    @Test
    void moveDirTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mkdir /dhfs_test/fuse/testdir").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testdir/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testdir/testf1").getStdout()));
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        Log.info("Moving");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "mkdir /dhfs_test/fuse/testdir2").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/testdir /dhfs_test/fuse/testdir2/testdirm").getExitCode());
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        Log.info("Reading");
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testdir2/testdirm/testf1").getStdout()));
    }


    // TODO: This probably shouldn't be working right now
    @Test
    void removeAddHostTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        var c2curl = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo rewritten > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo jioadsd > /dhfs_test/fuse/newfile1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo asvdkljm > /dhfs_test/fuse/newfile1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo dfgvh > /dhfs_test/fuse/newfile2").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo dscfg > /dhfs_test/fuse/newfile2").getExitCode());

        Log.info("Re-adding");
        container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> "rewritten\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "rewritten\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing removeAddHostTest");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/");
            Log.info(cat1);
            Log.info(cat2);
            Log.info(ls1);
            Log.info(ls2);

            return cat1.getStdout().contains("jioadsd") && cat1.getStdout().contains("asvdkljm") && cat1.getStdout().contains("dfgvh") && cat1.getStdout().contains("dscfg")
                    && cat2.getStdout().contains("jioadsd") && cat2.getStdout().contains("asvdkljm") && cat2.getStdout().contains("dfgvh") && cat2.getStdout().contains("dscfg");
        });
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getExitCode());
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /dhfs_test/fuse/testf"),
                Pair.of(container2, "echo test2 >> /dhfs_test/fuse/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var ls = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            Log.info(ls);
            Log.info(cat);
            return cat.getStdout().contains("test1") && cat.getStdout().contains("test2");
        });
    }

    @Test
    void dirCycleTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mkdir /dhfs_test/fuse/a").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mkdir /dhfs_test/fuse/b").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo xqr489 >> /dhfs_test/fuse/a/testfa").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo ahinou >> /dhfs_test/fuse/b/testfb").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls -lavh /dhfs_test/fuse").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var c2ls = container2.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -type f -exec cat {} \\;");
            return c2ls.getExitCode() == 0 && c2ls.getStdout().contains("xqr489") && c2ls.getStdout().contains("ahinou");
        });

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/a /dhfs_test/fuse/b").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/b /dhfs_test/fuse/a").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();


        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing dirCycleTest");
            Log.info(container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse"));
            Log.info(container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/a"));
            Log.info(container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/b"));
            Log.info(container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse"));
            Log.info(container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/a"));
            Log.info(container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/b"));

            var c1ls2 = container1.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -maxdepth 3 -type f -exec cat {} \\;");
            Log.info(c1ls2);
            var c2ls2 = container1.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -maxdepth 3 -type f -exec cat {} \\;");
            Log.info(c2ls2);

            return c1ls2.getStdout().contains("xqr489") && c1ls2.getStdout().contains("ahinou")
                    && c2ls2.getStdout().contains("xqr489") && c2ls2.getStdout().contains("ahinou")
                    && c1ls2.getExitCode() == 0 && c2ls2.getExitCode() == 0;
        });

    }

    @Test
    void removeAndMove() throws IOException, InterruptedException, TimeoutException {
        var client = DockerClientFactory.instance().client();
        Log.info("Creating");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        client.pauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 1);

        Log.info("Removing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "rm /dhfs_test/fuse/testf1").getExitCode());

        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 1);
        Log.info("Moving");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/testf1 /dhfs_test/fuse/testf2").getExitCode());
        Log.info("Listing");
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/").getExitCode());
        Log.info("Reading");
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf2").getStdout()));
        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 1);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 1);
        // Either removed, or moved
        // TODO: it always seems to be removed?
        Log.info("Reading both");
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse/");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            Log.info("cat1: " + cat1);
            Log.info("cat2: " + cat2);
            Log.info("ls1: " + ls1);
            Log.info("ls2: " + ls2);

            if (!ls1.getStdout().equals(ls2.getStdout())) {
                Log.info("Different ls?");
                return false;
            }

            if (ls1.getStdout().trim().isEmpty() && ls2.getStdout().trim().isEmpty()) {
                Log.info("Both empty");
                return true;
            }

            if (!cat1.getStdout().equals(cat2.getStdout())) {
                Log.info("Different cat?");
                return false;
            }

            if (!(cat1.getExitCode() == 0 && cat2.getExitCode() == 0 && ls1.getExitCode() == 0 && ls2.getExitCode() == 0)) {
                return false;
            }

            boolean hasMoved = cat1.getStdout().contains("tesempty") && cat2.getStdout().contains("tesempty")
                    && ls1.getStdout().contains("testf2") && !ls1.getStdout().contains("testf1")
                    && ls2.getStdout().contains("testf2") && !ls2.getStdout().contains("testf1");

            boolean removed = !cat1.getStdout().contains("tesempty") && !cat2.getStdout().contains("tesempty")
                    && !ls1.getStdout().contains("testf2") && !ls1.getStdout().contains("testf1")
                    && !ls2.getStdout().contains("testf2") && !ls2.getStdout().contains("testf1");

            if (hasMoved && removed) {
                Log.info("Both removed and moved");
                return false;
            }

            return hasMoved || removed;
        });
    }

}
