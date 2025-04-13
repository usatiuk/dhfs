package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import io.quarkus.logging.Log;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

public class KillIT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;

    String c1uuid;
    String c2uuid;

    @TempDir
    File data1;
    @TempDir
    File data2;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        Network network = Network.newNetwork();

        String testcontainersUser = System.getenv("TESTCONTAINERS_USER");

        container1 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network)
                .withFileSystemBind(data1.getAbsolutePath(), "/root/dhfs_default/data");
        if (testcontainersUser != null) {
            container1.withCreateContainerCmdModifier(cmd -> cmd.withUser(testcontainersUser));
        }
        container2 = new GenericContainer<>(DhfsImage.getInstance())
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig()).withDevices(Device.parse("/dev/fuse")))
                .waitingFor(Wait.forLogMessage(".*Listening.*", 1).withStartupTimeout(Duration.ofSeconds(60))).withNetwork(network)
                .withFileSystemBind(data2.getAbsolutePath(), "/root/dhfs_default/data");
        if (testcontainersUser != null) {
            container2.withCreateContainerCmdModifier(cmd -> cmd.withUser(testcontainersUser));
        }

        Stream.of(container1, container2).parallel().forEach(GenericContainer::start);

        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(KillIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(KillIT.class)).withPrefix("2-" + testInfo.getDisplayName());
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));

        c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();
        c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/data/stuff/self_uuid").getStdout();

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
    void killTest(TestInfo testInfo) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var barrier = new CyclicBarrier(2);
        var ret1 = executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.await();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /root/dhfs_default/fuse/test1; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Thread.sleep(10000);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        container1.start();
        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(KillIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing consistency");
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            Log.info(ls1);
            Log.info(cat1);
            Log.info(ls2);
            Log.info(cat2);

            return ls1.equals(ls2) && cat1.equals(cat2);
        });
    }

    @Test
    void killTestDirs(TestInfo testInfo) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var barrier = new CyclicBarrier(2);
        var ret1 = executor.submit(() -> {
            try {
                Log.info("Writing to container 1");
                barrier.await();
                container1.execInContainer("/bin/sh", "-c", "counter=0; while true; do counter=`expr $counter + 1`; echo $counter >> /root/dhfs_default/fuse/test$counter; done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Thread.sleep(10000);
        var client = DockerClientFactory.instance().client();
        client.killContainerCmd(container1.getContainerId()).exec();
        container1.stop();
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Lost connection to"), 60, TimeUnit.SECONDS);
        container1.start();
        waitingConsumer1 = new WaitingConsumer();
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(KillIT.class)).withPrefix("1-" + testInfo.getDisplayName());
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 60, TimeUnit.SECONDS);

        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            Log.info("Listing consistency");
            var ls1 = container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat1 = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            var ls2 = container2.execInContainer("/bin/sh", "-c", "ls /root/dhfs_default/fuse");
            var cat2 = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/*");
            Log.info(ls1);
            Log.info(cat1);
            Log.info(ls2);
            Log.info(cat2);

            return ls1.equals(ls2) && cat1.equals(cat2);
        });
    }
}
