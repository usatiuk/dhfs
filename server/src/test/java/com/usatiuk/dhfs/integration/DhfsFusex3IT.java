package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class DhfsFusex3IT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;
    GenericContainer<?> container3;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;
    WaitingConsumer waitingConsumer3;

    @BeforeEach
    void setup() throws IOException, InterruptedException, TimeoutException {
        String buildPath = System.getProperty("buildDirectory");
        System.out.println("Build path: " + buildPath);

        // TODO: Dedup
        Network network = Network.newNetwork();
        var image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("azul/zulu-openjdk-debian:21-jre-latest")
                                .run("apt update && apt install -y libfuse2 curl")
                                .copy("/app", "/app")
                                .cmd("java", "-ea", "-Xmx128M", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
                                        "-Ddhfs.objects.distributed.peerdiscovery.interval=500",
                                        "-Ddhfs.objects.distributed.invalidation.delay=200",
                                        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=4",
                                        "-Ddhfs.objects.ref_verification=true",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=TRACE",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".min-level=TRACE",
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
        container3 = new GenericContainer<>(image)
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);


        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::start);

        var c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();
        var c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();
        var c3uuid = container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class)).withPrefix(c1uuid);
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class)).withPrefix(c2uuid);
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer3 = new WaitingConsumer();
        var loggingConsumer3 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class)).withPrefix(c3uuid);
        container3.followOutput(loggingConsumer3.andThen(waitingConsumer3));

        Assertions.assertDoesNotThrow(() -> UUID.fromString(c1uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c2uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c3uuid));

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 4);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 4);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 4);

        var c1curl = container1.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        var c2curl1 = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        var c2curl3 = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c3uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        var c3curl = container3.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);

        Thread.sleep(2000);
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo test123 > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("test123\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Assertions.assertEquals("test123\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        boolean createFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container3, "echo test3 >> /root/dhfs_data/dhfs_fuse_root/testf")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(5000);
        for (var c : List.of(container1, container2, container3)) {
            var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
            var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
            Log.info(ls);
            Log.info(cat);
            Assertions.assertTrue(cat.getStdout().contains("test1"));
            Assertions.assertTrue(cat.getStdout().contains("test2"));
            Assertions.assertTrue(cat.getStdout().contains("test3"));
        }
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
    }


    @Test
    void dirConflictTest3() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        Assertions.assertEquals(0, container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getExitCode());
        boolean createFail = Stream.of(
                Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf"),
                Pair.of(container3, "echo test3 >> /root/dhfs_data/dhfs_fuse_root/testf")).parallel().map(p -> {
            try {
                Log.info("Creating");
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!createFail, "Failed creating one or more files");
        Thread.sleep(5000);
        for (var c : List.of(container1, container2, container3)) {
            var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
            var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
            Log.info(ls);
            Log.info(cat);
            Assertions.assertTrue(cat.getStdout().contains("test1"));
            Assertions.assertTrue(cat.getStdout().contains("test2"));
            Assertions.assertTrue(cat.getStdout().contains("test3"));
        }
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
    }

    @Test
    void fileConflictTest2() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo test123 > /root/dhfs_data/dhfs_fuse_root/testf1").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("test123\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());
        Assertions.assertEquals("test123\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        boolean writeFail = Stream.of(Pair.of(container1, "echo test1 >> /root/dhfs_data/dhfs_fuse_root/testf1"),
                Pair.of(container2, "echo test2 >> /root/dhfs_data/dhfs_fuse_root/testf1"),
                Pair.of(container3, "echo test3 >> /root/dhfs_data/dhfs_fuse_root/testf1")).parallel().map(p -> {
            try {
                return p.getLeft().execInContainer("/bin/sh", "-c", p.getRight()).getExitCode();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).anyMatch(r -> r != 0);
        Assumptions.assumeTrue(!writeFail, "Failed creating one or more files");
        Thread.sleep(5000);
        for (var c : List.of(container1, container2, container3)) {
            var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");
            var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*");
            Log.info(ls);
            Log.info(cat);
            Assertions.assertTrue(cat.getStdout().contains("test1"));
            Assertions.assertTrue(cat.getStdout().contains("test2"));
            Assertions.assertTrue(cat.getStdout().contains("test3"));
        }
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root").getStdout());
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/*").getStdout());
    }

}
