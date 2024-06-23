package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

    @BeforeEach
    void setup() {
        String buildPath = System.getProperty("buildDirectory");
        System.out.println("Build path: " + buildPath);
        Network network = Network.newNetwork();
        var image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("azul/zulu-openjdk-alpine:21-jre-latest")
                                .run("apk add --update fuse curl strace")
                                .copy("/app", "/app")
                                .cmd("java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
                                        "-Ddhfs.objects.distributed.peerdiscovery.interval=1000", "-jar", "/app/quarkus-run.jar")
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
        var loggingConsumer1 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFuseIT.class));
        container1.followOutput(loggingConsumer1.andThen(waitingConsumer1));
        waitingConsumer2 = new WaitingConsumer();
        var loggingConsumer2 = new Slf4jLogConsumer(LoggerFactory.getLogger(DhfsFuseIT.class));
        container2.followOutput(loggingConsumer2.andThen(waitingConsumer2));
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        var c1uuid = container1.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();
        var c2uuid = container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_root_d/self_uuid").getStdout();
        System.out.println(container1.execInContainer("/bin/sh", "-c", "uname -a"));
        System.out.println(container1.execInContainer("/bin/sh", "-c", "strace ls /root/dhfs_data/dhfs_fuse_root"));
//        System.out.println(container2.execInContainer("/bin/sh", "-c", "strace ls /root/dhfs_data/dhfs_fuse_root"));

        Assertions.assertDoesNotThrow(() -> UUID.fromString(c1uuid));
        Assertions.assertDoesNotThrow(() -> UUID.fromString(c2uuid));

        var c1curl = container1.execInContainer("/bin/sh", "-c",
                "curl --header \"Content-Type: application/json\" " +
                        "  --request PUT " +
                        "  --data '{\"uuid\":\"" + c2uuid + "\"}' " +
                        "  http://localhost:8080/objects-manage/known-peers");
        System.out.println(c1curl);
        var c1res = container1.execInContainer("/bin/sh", "-c",
                "curl http://localhost:8080/objects-manage/known-peers");
        var c2res = container2.execInContainer("/bin/sh", "-c",
                "curl http://localhost:8080/objects-manage/known-peers");
        System.out.println(c1res);
        System.out.println(c2res);

        waitingConsumer2.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 30, TimeUnit.SECONDS);
        waitingConsumer1.waitUntil(frame -> frame.getUtf8String().contains("Connected"), 30, TimeUnit.SECONDS);

        Thread.sleep(10000);

        var write = container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root");

        System.out.println(container1.execInContainer("/bin/sh", "-c", "ls /root/dhfs_data/dhfs_fuse_root"));

        Assertions.assertEquals("test123", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_data/dhfs_fuse_root/testf1").getStdout());

        Thread.sleep(10000);
    }
}
