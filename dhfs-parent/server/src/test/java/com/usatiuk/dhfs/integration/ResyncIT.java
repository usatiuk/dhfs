package com.usatiuk.dhfs.integration;

import com.github.dockerjava.api.model.Device;
import io.quarkus.logging.Log;
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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class ResyncIT {
    GenericContainer<?> container1;
    GenericContainer<?> container2;

    WaitingConsumer waitingConsumer1;
    WaitingConsumer waitingConsumer2;

    String c1uuid;
    String c2uuid;

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException, InterruptedException, TimeoutException {
        String buildPath = System.getProperty("buildDirectory");
        String nativeLibsDirectory = System.getProperty("nativeLibsDirectory");
        Log.info("Build path: " + buildPath);
        Log.info("Native libs path: " + nativeLibsDirectory);

        Network network = Network.newNetwork();
        var image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("azul/zulu-openjdk-debian:21-jre-latest")
                                .run("apt update && apt install -y libfuse2 curl")
                                .copy("/app", "/app")
                                .copy("/libs", "/libs")
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
                                        "-Dcom.usatiuk.dhfs.supportlib.native-path=/libs",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=TRACE",
                                        "-jar", "/app/quarkus-run.jar")
                                .build())
                .withFileFromPath("/app", Paths.get(buildPath, "quarkus-app"))
                .withFileFromPath("/libs", Paths.get(nativeLibsDirectory));
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
    }

    @AfterEach
    void stop() {
        Stream.of(container1, container2).parallel().forEach(GenericContainer::stop);
    }

    @Test
    void readWriteFileTest() throws IOException, InterruptedException, TimeoutException {
        Assertions.assertEquals(0, container1.execInContainer("/bin/sh", "-c", "echo tesempty > /root/dhfs_default/fuse/testf1").getExitCode());
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
        Thread.sleep(2000);
        Assertions.assertEquals("tesempty\n", container2.execInContainer("/bin/sh", "-c", "cat /root/dhfs_default/fuse/testf1").getStdout());
    }

}
