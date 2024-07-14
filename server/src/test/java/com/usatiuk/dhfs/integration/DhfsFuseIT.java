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
                                        "-Ddhfs.objects.peerdiscovery.interval=500",
                                        "-Ddhfs.objects.invalidation.delay=200",
                                        "-Ddhfs.objects.ref_verification=true",
                                        "-Ddhfs.objects.deletion.delay=0",
                                        "-Ddhfs.objects.sync.timeout=5",
                                        "-Ddhfs.objects.reconnect_interval=1s",
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

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();

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
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
    }

    @Test
    void readWriteRewriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
    }

    @Test
    void createDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo newfile > /root/dhfs_data/dhfs_fuse_root/testf2").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("newfile\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf2").getStdout());
        Assertions.assertEquals("newfile\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf2").getStdout());
    }

    @Test
    void writeRewriteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Assertions.assertEquals("rewritten\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
    }

    @Test
    void deleteDelayedTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);

        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "rm /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Delaying deletion check"), 60, TimeUnit.SECONDS, 1);

        client.unpauseContainerCmd(container2.getContainerId()).exec();

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(1000);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Thread.sleep(1000);

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 1);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        Thread.sleep(1000);
        Assertions.assertEquals(1, container2.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Assertions.assertEquals(1, container1.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
    }

    @Test
    void deleteTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());


        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "rm /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 1);

        Thread.sleep(500);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Thread.sleep(500);

        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Deleting from persistent"), 60, TimeUnit.SECONDS, 3);

        Thread.sleep(1000);
        Assertions.assertEquals(1, container2.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Assertions.assertEquals(1, container1.execInContainer("/bin/sh", "-c", "test -f /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
    }

    @Test
    void moveFileTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root/").getExitCode());
        Thread.sleep(1000);
        Log.info("Moving");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mv /root/dhfs_data/dhfs_fuse_root/testf1 /root/dhfs_data/dhfs_fuse_root/testf2").getExitCode());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root/").getExitCode());
        Thread.sleep(1000);
        Log.info("Reading");
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf2").getStdout());
    }

    @Test
    void moveDirTest() throws IOException, InterruptedException, TimeoutException {
        Log.info("Creating");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_data/dhfs_fuse_root/testdir").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testdir/testf1").getExitCode());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testdir/testf1").getStdout());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root/").getExitCode());
        Thread.sleep(1000);
        Log.info("Moving");
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mkdir /root/dhfs_data/dhfs_fuse_root/testdir2").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "mv /root/dhfs_data/dhfs_fuse_root/testdir /root/dhfs_data/dhfs_fuse_root/testdir2/testdirm").getExitCode());
        Thread.sleep(1000);
        Log.info("Listing");
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root/").getExitCode());
        Thread.sleep(1000);
        Log.info("Reading");
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testdir2/testdirm/testf1").getStdout());
    }


    @Test
    void removeAddHostTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        var c2curl = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(1000);
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        Thread.sleep(2000);
        Assertions.assertEquals("rewritten\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Thread.sleep(1000);
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(1000);
        var ls = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
        var cat = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
        Log.info(ls);
        Log.info(cat);
        Assertions.assertTrue(cat.getStdout().contains("test1"));
        Assertions.assertTrue(cat.getStdout().contains("test2"));
//        Assertions.assertTrue(ls.getStdout().chars().filter(c -> c == '\n').count() >= 2);
    }

    @Test
    void dirConflictTest2() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(1000);
        var ls = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
        var cat = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
        Log.info(ls);
        Log.info(cat);
        Assertions.assertTrue(cat.getStdout().contains("test1"));
        Assertions.assertTrue(cat.getStdout().contains("test2"));
    }

    @Test
    void dirConflictTest3() throws IOException, InterruptedException, TimeoutException {
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(1000);
        var ls = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
        var cat = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
        Log.info(ls);
        Log.info(cat);
        Assertions.assertTrue(cat.getStdout().contains("test1"));
        Assertions.assertTrue(cat.getStdout().contains("test2"));
    }

    @Test
    void dirConflictTest4() throws IOException, InterruptedException, TimeoutException {
        boolean createdOk = (container1.execInContainer("/bin/sh", "-c", "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf").getExitCode() == 0)
                && (container2.execInContainer("/bin/sh", "-c", "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf").getExitCode() == 0);
        Assumptions.assumeTrue(createdOk, "Failed creating one or more files");
        Thread.sleep(1000);
        var ls = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
        var cat = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
        Log.info(ls);
        Log.info(cat);
        Assertions.assertTrue(cat.getStdout().contains("test1"));
        Assertions.assertTrue(cat.getStdout().contains("test2"));
    }

}
