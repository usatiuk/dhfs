package com.usatiuk.dhfs.integration;

import io.quarkus.logging.Log;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DhfsImage implements Future<String> {

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public String get() throws InterruptedException, ExecutionException {
        return buildImpl();
    }

    @Override
    public String get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return buildImpl();
    }

    private static String _builtImage = null;

    private synchronized String buildImpl() {
        if (_builtImage != null) {
            return _builtImage;
        }

        String buildPath = System.getProperty("buildDirectory");
        String nativeLibsDirectory = System.getProperty("nativeLibsDirectory");
        Log.info("Build path: " + buildPath);
        Log.info("Native libs path: " + nativeLibsDirectory);

        var image = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("azul/zulu-openjdk-debian:21-jre-headless-latest")
                                .run("apt update && apt install -y libfuse2 curl gcc")
                                .copy("/app", "/app")
                                .copy("/libs", "/libs")
                                .cmd("java", "-ea", "-Xmx128M",
                                        "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
                                        "--add-exports", "java.base/jdk.internal.access=ALL-UNNAMED",
                                        "-Ddhfs.objects.peerdiscovery.interval=100",
                                        "-Ddhfs.objects.invalidation.delay=100",
                                        "-Ddhfs.objects.deletion.delay=0",
                                        "-Ddhfs.objects.deletion.can-delete-retry-delay=1000",
                                        "-Ddhfs.objects.ref_verification=true",
                                        "-Ddhfs.objects.write_log=true",
                                        "-Ddhfs.objects.sync.timeout=10",
                                        "-Ddhfs.objects.sync.ping.timeout=5",
                                        "-Ddhfs.objects.reconnect_interval=1s",
                                        "-Dcom.usatiuk.dhfs.supportlib.native-path=/libs",
                                        "-Dquarkus.log.category.\"com.usatiuk\".level=TRACE",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=TRACE",
                                        "-jar", "/app/quarkus-run.jar")
                                .build())
                .withFileFromPath("/app", Paths.get(buildPath, "quarkus-app"))
                .withFileFromPath("/libs", Paths.get(nativeLibsDirectory));

        _builtImage = image.get();
        Log.info("Image built: " + _builtImage);
        return _builtImage;
    }

    private DhfsImage() {}

    private static DhfsImage INSTANCE = new DhfsImage();

    public static DhfsImage getInstance() {
        return INSTANCE;
    }
}
