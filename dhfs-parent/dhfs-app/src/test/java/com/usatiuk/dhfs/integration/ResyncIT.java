package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;
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

public class ResyncIT {
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
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty > /dhfs_test/fuse/testf1").getExitCode());
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

        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testf1").getStdout()));
    }


    @Test
    void manyFiles() throws IOException, InterruptedException, TimeoutException {
        var ret = container1.execInContainer("/bin/sh", "-c", "for i in $(seq 1 200); do echo $i > /dhfs_test/fuse/test$i; done");
        Assertions.assertEquals(0, ret.getExitCode());
        var foundWc = container1.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -type f | wc -l");
        Assertions.assertEquals(200, Integer.valueOf(foundWc.getStdout().strip()));

        ret = container2.execInContainer("/bin/sh", "-c", "for i in $(seq 1 200); do echo $i > /dhfs_test/fuse/test-2-$i; done");
        Assertions.assertEquals(0, ret.getExitCode());
        foundWc = container2.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -type f | wc -l");
        Assertions.assertEquals(200, Integer.valueOf(foundWc.getStdout().strip()));

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
        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            var foundWc2 = container2.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -type f | wc -l");
            return 400 == Integer.valueOf(foundWc2.getStdout().strip());
        });
        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            var foundWc2 = container1.execInContainer("/bin/sh", "-c", "find /dhfs_test/fuse -type f | wc -l");
            return 400 == Integer.valueOf(foundWc2.getStdout().strip());
        });
    }

    @Test
    void folderAfterMove() throws IOException, InterruptedException, TimeoutException {
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mkdir /dhfs_test/fuse/testd1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty1 > /dhfs_test/fuse/testd1/testf1").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "mv /dhfs_test/fuse/testd1 /dhfs_test/fuse/testd2").getExitCode());
        await().atMost(45, TimeUnit.SECONDS).until(() -> 0 == container1.execInContainer("/bin/sh", "-c", "echo tesempty2 > /dhfs_test/fuse/testd2/testf2").getExitCode());

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

        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty1\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testd2/testf1").getStdout()));
        await().atMost(45, TimeUnit.SECONDS).until(() -> "tesempty2\n".equals(container2.execInContainer("/bin/sh", "-c", "cat /dhfs_test/fuse/testd2/testf2").getStdout()));
    }

}
