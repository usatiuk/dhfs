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
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class DhfsFuseIT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;

    String c1uuid;
    String c2uuid;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        String buildPath = System.getProperty("buildDirectory");
        System.out.println("Build path: " + buildPath);

        Network network = Network.newNetwork();
        var image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("azul/zulu-openjdk-debian:21-jre-latest")
                                .run("apt update && apt install -y libfuse2 curl")
                                .copy("/app", "/app")
                                .cmd("java", "-ea", "-Xmx128M", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
                                        "--add-exports", "java.base/jdk.internal.access=ALL-UNNAMED",
                                        "-Ddhfs.objects.peerdiscovery.interval=100",
                                        "-Ddhfs.objects.invalidation.delay=100",
                                        "-Ddhfs.objects.ref_verification=true",
                                        "-Ddhfs.objects.deletion.delay=0",
                                        "-Ddhfs.objects.write_log=true",
                                        "-Ddhfs.objects.sync.timeout=20",
                                        "-Ddhfs.objects.sync.ping.timeout=20",
                                        "-Ddhfs.objects.reconnect_interval=1s",
                                        "-Dquarkus.log.category.\"com.usatiuk\".level=TRACE",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=TRACE",
                                        "-jar", "/app/quarkus-run.jar")
                                .build())
                .withFileFromPath("/app", Paths.get(buildPath, "quarkus-app"));
        container1 = new GenericContainer<>(image)
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);
        container2 = new GenericContainer<>(image)
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

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();

        Assertions.assertDoesNotThrow(() -> UUID.fromString(c1uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c2uuid));

        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS);

        var c1curl = container1.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        var c2curl = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(1000);
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

    @Test
    void readWriteRewriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

    @Test
    void createDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo newfile > /root/dhfs_default/fuse/testf2").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("newfile\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf2").getStdout());
        Assertions.assertEquals("newfile\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf2").getStdout());
    }

    @Test
    void writeRewriteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_default/fuse/testf1").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("rewritten\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

    // TODO: How this fits with the tree?
    @Test
    @Disabled
    void deleteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/testf1").getExitCode());
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Delaying deletion check"), 60, TimeUnit.SECONDS, 1);

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(1000);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getExitCode());
        Thread.sleep(1000);

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 1);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        Thread.sleep(1000);
        Assertions.assertEquals(1, container2.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_default/fuse/testf1").getExitCode());
        Assertions.assertEquals(1, container1.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_default/fuse/testf1").getExitCode());
    }

    @Test
    void deleteTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());


        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(500);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getExitCode());
        Thread.sleep(500);

        // Motivate the log a little
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty2 > /root/dhfs_default/fuse/testf2").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo tesempty3 > /root/dhfs_default/fuse/testf3").getExitCode());

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        Thread.sleep(1000);
        Assertions.assertEquals(1, container2.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_default/fuse/testf1").getExitCode());
        Assertions.assertEquals(1, container1.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_default/fuse/testf1").getExitCode());
    }

    @Test
    void moveFileTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/").getExitCode());
        Thread.sleep(1000);
        Log.info("Moving");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mv /root/dhfs_default/fuse/testf1 /root/dhfs_default/fuse/testf2").getExitCode());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/").getExitCode());
        Thread.sleep(1000);
        Log.info("Reading");
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf2").getStdout());
    }

    @Test
    void moveDirTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_default/fuse/testdir").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testdir/testf1").getExitCode());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testdir/testf1").getStdout());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/").getExitCode());
        Thread.sleep(1000);
        Log.info("Moving");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_default/fuse/testdir2").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mv /root/dhfs_default/fuse/testdir /root/dhfs_default/fuse/testdir2/testdirm").getExitCode());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/").getExitCode());
        Thread.sleep(1000);
        Log.info("Reading");
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testdir2/testdirm/testf1").getStdout());
    }


    // TODO: This probably shouldn't be working right now
    @Test
    void removeAddHostTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        var c2curl = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_default/fuse/testf1").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo jioadsd > /root/dhfs_default/fuse/newfile1").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo asvdkljm > /root/dhfs_default/fuse/newfile1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo dfgvh > /root/dhfs_default/fuse/newfile2").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo dscfg > /root/dhfs_default/fuse/newfile2").getExitCode());

        container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("rewritten\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
        var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
        var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/");
        var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/");
        Log.info(cat1);
        Log.info(cat2);
        Log.info(ls1);
        Log.info(ls2);

        Assertions.assertTrue(cat1.getStdout().contains("jioadsd") && cat1.getStdout().contains("asvdkljm") && cat1.getStdout().contains("dfgvh") && cat1.getStdout().contains("dscfg"));
        Assertions.assertTrue(cat2.getStdout().contains("jioadsd") && cat2.getStdout().contains("asvdkljm") && cat2.getStdout().contains("dfgvh") && cat2.getStdout().contains("dscfg"));
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getExitCode());
        Thread.sleep(1000);
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_default/fuse/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_default/fuse/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(1000);
        var ls = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
        var cat = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
        Log.info(ls);
        Log.info(cat);
        Assertions.assertTrue(cat.getStdout().contains("test1"));
        Assertions.assertTrue(cat.getStdout().contains("test2"));
//        Assertions.assertTrue(ls.getStdout().chars().filter(c -> c == '\n').count() >= 2);
    }

    @Test
    void dirCycleTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_default/fuse/a").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_default/fuse/b").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo xqr489 >> /root/dhfs_default/fuse/a/testfa").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo ahinou >> /root/dhfs_default/fuse/b/testfb").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls -lavh /root/dhfs_default/fuse").getExitCode());
        var c2ls = container2.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/fuse -type f -exec cat {} \\;");
        Assertions.assertEquals(0, c2ls.getExitCode());
        Assertions.assertTrue(c2ls.getStdout().contains("xqr489"));
        Assertions.assertTrue(c2ls.getStdout().contains("ahinou"));
        Thread.sleep(1000);

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mv /root/dhfs_default/fuse/a /root/dhfs_default/fuse/b").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "mv /root/dhfs_default/fuse/b /root/dhfs_default/fuse/a").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();

        Thread.sleep(10000);
        Log.info(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse"));
        Log.info(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/a"));
        Log.info(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/b"));
        Log.info(container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse"));
        Log.info(container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/a"));
        Log.info(container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse/b"));
        // FIXME: An infinite cycle can indeed be created
        var c1ls2 = container1.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/fuse -maxdepth 3 -type f -exec cat {} \\;");
        Log.info(c1ls2);
        Assertions.assertEquals(0, c1ls2.getExitCode());
        Assertions.assertTrue(c1ls2.getStdout().contains("xqr489"));
        Assertions.assertTrue(c1ls2.getStdout().contains("ahinou"));
        var c2ls2 = container1.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/fuse -maxdepth 3 -type f -exec cat {} \\;");
        Log.info(c2ls2);
        Assertions.assertEquals(0, c2ls2.getExitCode());
        Assertions.assertTrue(c2ls2.getStdout().contains("xqr489"));
        Assertions.assertTrue(c2ls2.getStdout().contains("ahinou"));
    }

}
