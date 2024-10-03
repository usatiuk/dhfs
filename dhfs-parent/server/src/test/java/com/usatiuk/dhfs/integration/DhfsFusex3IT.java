package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import io.quarkus.logging.Log;
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

    String c1uuid;
    String c2uuid;
    String c3uuid;

    long emptyFileCount;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        // TODO: Dedup
        Network network = Network.newNetwork();

        container1 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);
        container2 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);
        container3 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network);


        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::start);

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();
        c3uuid = container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();

        Log.info(container1.getContainerId() + "=" + c1uuid);
        Log.info(container2.getContainerId() + "=" + c2uuid);
        Log.info(container3.getContainerId() + "=" + c3uuid);

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class))
                .withPrefix(c1uuid.substring(0, 4) + "-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class))
                .withPrefix(c2uuid.substring(0, 4) + "-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
        waitingConsumer3 = new WaitingConsumer();
        var loggingConsumer3 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFusex3IT.class))
                .withPrefix(c3uuid.substring(0, 4) + "-" + testInfo.getDisplayName());
        container3.followOutput(loggingConsumer3.andThen(waitingConsumer3));

        Assertions.assertDoesNotThrow(() -> UUID.fromString(c1uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c2uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c3uuid));

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Ignoring new address"), 60, TimeUnit.SECONDS, 2);

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

        emptyFileCount = Integer.valueOf(container1.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/data/objs -type f | wc -l").getStdout().strip());
    }

    private void checkEmpty() throws IOException, InterruptedException {
        for (var container : List.of(container1, container2, container3)) {
            var found = container.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/data/objs -type f");
            var foundWc = container.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/data/objs -type f | wc -l");
            Log.info("Remaining objects in " + container.getContainerId() + ": " + found.toString() + " " + foundWc.toString());
            Assertions.assertEquals(0, found.getExitCode());
            Assertions.assertEquals(0, foundWc.getExitCode());
            Assertions.assertEquals(emptyFileCount, Integer.parseInt(foundWc.getStdout().strip()));
        }
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

    @Test
    void largerFileDeleteTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && curl -O https://ash-speed.hetzner.com/100MB.bin").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "head -c 10 /root/dhfs_default/fuse/100MB.bin").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals(0, container3.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/100MB.bin").getExitCode());
        Thread.sleep(10000);
        checkEmpty();
    }

    @Test
    void largerFileDeleteTestNoDelays() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && curl -O https://ash-speed.hetzner.com/100MB.bin").getExitCode());
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "head -c 10 /root/dhfs_default/fuse/100MB.bin").getExitCode());
        Assertions.assertEquals(0, container3.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/100MB.bin").getExitCode());
        Thread.sleep(10000);
        checkEmpty();
    }

    @Test
    void gccHelloWorldTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo '#include<stdio.h>\nint main(){printf(\"hello world\"); return 0;}' > /root/dhfs_default/fuse/hello.c").getExitCode());
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && gcc hello.c").getExitCode());
        var helloOut1 = container1.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
        Log.info(helloOut1);
        Assertions.assertEquals(0, helloOut1.getExitCode());
        Assertions.assertEquals("hello world", helloOut1.getStdout());
        Thread.sleep(2000);
        var helloOut2 = container2.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
        Log.info(helloOut2);
        Assertions.assertEquals(0, helloOut2.getExitCode());
        Assertions.assertEquals("hello world", helloOut2.getStdout());
        var helloOut3 = container3.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
        Log.info(helloOut3);
        Assertions.assertEquals(0, helloOut3.getExitCode());
        Assertions.assertEquals("hello world", helloOut3.getStdout());
    }

    @Test
    void removeHostTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());

        var c3curl = container3.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        Thread.sleep(2000);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_default/fuse/testf1").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("rewritten\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
        Assertions.assertEquals("tesempty\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        client.pauseContainerCmd(container2.getContainerId()).exec();
        // Pauses needed as otherwise docker buffers some incoming packets
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container3.execInContainer("/bin/sh", "-c", "echo test3 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container3.getContainerId()).exec();
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo test2 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo test1 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container3.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);

        Thread.sleep(2000);
        for (var c : List.of(container1, container2, container3)) {
            var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            Log.info(ls);
            Log.info(cat);
            Assertions.assertTrue(cat.getStdout().contains("test1"));
            Assertions.assertTrue(cat.getStdout().contains("test2"));
            Assertions.assertTrue(cat.getStdout().contains("test3"));
        }
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout());
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
    }

    @Test
    void fileConflictTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf").getExitCode());
        Thread.sleep(2000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf").getStdout());
        Assertions.assertEquals("tesempty\n", container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf").getStdout());
        Thread.sleep(1000);

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        client.pauseContainerCmd(container2.getContainerId()).exec();
        // Pauses needed as otherwise docker buffers some incoming packets
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container3.execInContainer("/bin/sh", "-c", "echo test3 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container3.getContainerId()).exec();
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container2.execInContainer("/bin/sh", "-c", "echo test2 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo test1 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container3.getContainerId()).exec();
        Log.warn("Waiting for connections");
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        Log.warn("Connected");

        Thread.sleep(20000);
        for (var c : List.of(container1, container2, container3)) {
            var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            Log.info(ls);
            Log.info(cat);
            Assertions.assertTrue(cat.getStdout().contains("test1"));
            Assertions.assertTrue(cat.getStdout().contains("test2"));
            Assertions.assertTrue(cat.getStdout().contains("test3"));
        }
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout());
        Assertions.assertEquals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
        Assertions.assertEquals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout(),
                container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
    }

}
