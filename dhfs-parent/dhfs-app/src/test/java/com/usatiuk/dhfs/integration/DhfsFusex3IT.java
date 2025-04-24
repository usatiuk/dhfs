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

    Network network;

    // This calculation is somewhat racy, so keep it hardcoded for now
    long emptyFileCount = 9;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        // TODO: Dedup
        network = Network.newNetwork();

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

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/data/stuff/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/data/stuff/self_uuid").getStdout();
        c3uuid = container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/data/stuff/self_uuid").getStdout();

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
                        "  http://localhost:8080/peers-manage/known-peers");

        var c2curl1 = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c1uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        var c2curl3 = container2.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c3uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        var c3curl = container3.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
    }

    private boolean checkEmpty() throws IOException, InterruptedException {
        for (var container : List.of(container1, container2, container3)) {
            var found = container.execInContainer("/bin/sh", "-c", "find /dhfs_test/data/objs -type f");
            var foundWc = container.execInContainer("/bin/sh", "-c", "find /dhfs_test/data/objs -type f | wc -l");
            Log.info("Remaining objects in " + container.getContainerId() + ": " + found.toString() + " " + foundWc.toString());
            if (!(found.getExitCode() == 0 && foundWc.getExitCode() == 0 && Integer.parseInt(foundWc.getStdout().strip()) == emptyFileCount))
                return false;
        }
        return true;
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2, container3).parallel().forEach(GenericContainer::stop);
        network.close();
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }

    // FIXME:
    @Test
    @Disabled
    void largerFileDeleteTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /dhfs_test/fuse && dd if=/dev/urandom of=10MB.bin bs=1M count=10").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "head -c 10 /dhfs_test/fuse/10MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "rm /dhfs_test/fuse/10MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> checkEmpty());
    }

    @Test
    @Disabled
    void largerFileDeleteTestNoDelays() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /dhfs_test/fuse && dd if=/dev/urandom of=10MB.bin bs=1M count=10").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "head -c 10 /dhfs_test/fuse/10MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "rm /dhfs_test/fuse/10MB.bin").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> checkEmpty());
    }

    @Test
    void gccHelloWorldTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo '#include<stdio.h>\nint main(){printf(\"hello world\"); return 0;}' > /dhfs_test/fuse/hello.c").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "cd /dhfs_test/fuse && gcc hello.c").getExitCode());

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container1.execInContainer("/bin/sh", "-c", "/dhfs_test/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container2.execInContainer("/bin/sh", "-c", "/dhfs_test/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            var helloOut = container3.execInContainer("/bin/sh", "-c", "/dhfs_test/fuse/a.out");
            Log.info(helloOut);
            return helloOut.getExitCode() == 0 && helloOut.getStdout().equals("hello world");
        });
    }

    @Test
    void removeHostTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));

        var c3curl = container3.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request DELETE " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/peers-manage/known-peers");

        Thread.sleep(10000);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo rewritten > /dhfs_test/fuse/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "rewritten\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }

    @Test
    void dirConflictTest() throws IOException, InterruptedException, TimeoutException {
        var client = DockerClientFactory.instance().client();
        client.pauseContainerCmd(container1.getContainerId()).exec();
        client.pauseContainerCmd(container2.getContainerId()).exec();
        // Pauses needed as otherwise docker buffers some incoming packets
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "echo test3 >> /dhfs_test/fuse/testf").getExitCode());
        client.pauseContainerCmd(container3.getContainerId()).exec();
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo test2 >> /dhfs_test/fuse/testf").getExitCode());
        client.pauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container1.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo test1 >> /dhfs_test/fuse/testf").getExitCode());
        client.unpauseContainerCmd(container2.getContainerId()).exec();
        client.unpauseContainerCmd(container3.getContainerId()).exec();
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            for (var c : List.of(container1, container2, container3)) {
                var ls = c.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
                var cat = c.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
                Log.info(ls);
                Log.info(cat);
                if (!(cat.getStdout().contains("test1") && cat.getStdout().contains("test2") && cat.getStdout().contains("test3")))
                    return false;
            }
            return true;
        });

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            return container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getStdout().equals(
                    container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse").getStdout()) &&
                    container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*").getStdout().equals(
                            container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*").getStdout());
        });
    }

    @Test
    void fileConflictTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf").getStdout()));

        var client = DockerClientFactory.instance().client();
        client.disconnectFromNetworkCmd().withContainerId(container1.getContainerId()).withNetworkId(network.getId()).exec();
        client.disconnectFromNetworkCmd().withContainerId(container2.getContainerId()).withNetworkId(network.getId()).exec();
        client.disconnectFromNetworkCmd().withContainerId(container3.getContainerId()).withNetworkId(network.getId()).exec();

        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS, 2);

        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container3.execInContainer("/bin/sh", "-c", "echo test3 >> /dhfs_test/fuse/testf").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container2.execInContainer("/bin/sh", "-c", "echo test2 >> /dhfs_test/fuse/testf").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo test1 >> /dhfs_test/fuse/testf").getExitCode());

        client.connectToNetworkCmd().withContainerId(container1.getContainerId()).withNetworkId(network.getId()).exec();
        client.connectToNetworkCmd().withContainerId(container2.getContainerId()).withNetworkId(network.getId()).exec();
        client.connectToNetworkCmd().withContainerId(container3.getContainerId()).withNetworkId(network.getId()).exec();

        Log.warn("Waiting for connections");
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        waitingConsumer3.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS, 2);
        Log.warn("Connected");

        // TODO: There's some issue with cache, so avoid file reads
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing consistency 1");
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var ls3 = container3.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            Log.info(ls1);
            Log.info(ls2);
            Log.info(ls3);

            return (ls1.getExitCode() == 0 && ls2.getExitCode() == 0 && ls3.getExitCode() == 0)
                    && (ls1.getStdout().equals(ls2.getStdout()) && ls2.getStdout().equals(ls3.getStdout()));
        });

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing");
            for (var c : List.of(container1, container2, container3)) {
                var ls = c.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
                var cat = c.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
                Log.info(ls);
                Log.info(cat);
                if (!(cat.getExitCode() == 0 && ls.getExitCode() == 0))
                    return false;
                if (!(cat.getStdout().contains("test1") && cat.getStdout().contains("test2") && cat.getStdout().contains("test3")))
                    return false;
            }
            return true;
        });

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing consistency");
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            var ls3 = container3.execInContainer("/bin/sh", "-c", "ls /dhfs_test/fuse");
            var cat3 = container3.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/*");
            Log.info(ls1);
            Log.info(cat1);
            Log.info(ls2);
            Log.info(cat2);
            Log.info(ls3);
            Log.info(cat3);

            return (ls1.getExitCode() == 0 && ls2.getExitCode() == 0 && ls3.getExitCode() == 0)
                    && (cat1.getExitCode() == 0 && cat2.getExitCode() == 0 && cat3.getExitCode() == 0)
                    && (cat1.getStdout().equals(cat2.getStdout()) && cat2.getStdout().equals(cat3.getStdout()))
                    && (ls1.getStdout().equals(ls2.getStdout()) && ls2.getStdout().equals(ls3.getStdout()));
        });
    }

}
