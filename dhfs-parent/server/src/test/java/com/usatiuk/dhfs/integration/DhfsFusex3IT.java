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

import static org.awaitility.Awaitility.await;

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

    // This calculation is somewhat racy, so keep it hardcoded for now
    long emptyFileCount = 9;

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

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("New address"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("New address"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("New address"), 60, TimeUnit.SECONDS, 2);

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
    }

    private boolean checkEmpty() throws IOException, InterruptedException {
        for (var container : List.of(container1, container2, container3)) {
            var found = container.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/data/objs -type f");
            var foundWc = container.execInContainer("/bin/sh", "-c", "find /root/dhfs_default/data/objs -type f | wc -l");
            Log.info("Remaining objects in " + container.getContainerId() + ": " + found.toString() + " " + foundWc.toString());
            if (!(found.getExitCode() == 0 && foundWc.getExitCode() == 0 && Integer.parseInt(foundWc.getStdout().strip()) == emptyFileCount))
                return false;
        }
        return true;
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
    }

    @Test
    void largerFileDeleteTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && curl -O https://ash-speed.hetzner.com/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "head -c 10 /root/dhfs_default/fuse/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> checkEmpty());
    }

    @Test
    void largerFileDeleteTestNoDelays() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && curl -O https://ash-speed.hetzner.com/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "head -c 10 /root/dhfs_default/fuse/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "rm /root/dhfs_default/fuse/100MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> checkEmpty());
    }

    @Test
    void gccHelloWorldTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo '#include<stdio.h>\nint main(){printf(\"hello world\"); return 0;}' > /root/dhfs_default/fuse/hello.c").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /root/dhfs_default/fuse && gcc hello.c").getExitCode());

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container1.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container2.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container3.execInContainer("/bin/sh", "-c", "/root/dhfs_default/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
    }

    @Test
    void removeHostTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));

        var c3curl = container3.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo rewritten > /root/dhfs_default/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "rewritten\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout()));
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        client.pauseContainerCmd(container2.getContainerId()).exec();
        // Pauses needed as otherwise docker buffers some incoming packets
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "echo test3 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container3.getContainerId()).exec();
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo test2 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo test1 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container3.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            for (var c : List.of(container1, container2, container3)) {
                var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
                var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
                Log.info(ls);
                Log.info(cat);
                if (!(cat.getStdout().contains("test1") && cat.getStdout().contains("test2") && cat.getStdout().contains("test3")))
                    return false;
            }
            return true;
        });

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            return container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout().equals(
                    container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
        });
    }

    @Test
    void fileConflictTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf").getStdout()));

        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        client.pauseContainerCmd(container2.getContainerId()).exec();
        // Pauses needed as otherwise docker buffers some incoming packets
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "echo test3 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container3.getContainerId()).exec();
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo test2 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo test1 >> /root/dhfs_default/fuse/testf").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container3.getContainerId()).exec();
        Log.warn("Waiting for connections");
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        Log.warn("Connected");

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            for (var c : List.of(container1, container2, container3)) {
                var ls = c.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
                var cat = c.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
                Log.info(ls);
                Log.info(cat);
                if (!(cat.getStdout().contains("test1") && cat.getStdout().contains("test2") && cat.getStdout().contains("test3")))
                    return false;
            }
            return true;
        });

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            return container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout().equals(
                    container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*").getStdout());
        });
    }

}
