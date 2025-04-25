package com.usatiuk.dhfsapp.integration;

import io.quarkus.logging.Log;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DhfsImage implements Future<String> {

    private static final DhfsImage INSTANCE = new DhfsImage();
    private static String _builtImage = null;

    private DhfsImage() {
    }

    public static DhfsImage getInstance() {
        return INSTANCE;
    }

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

    private synchronized String buildImpl() {
        if (_builtImage != null) {
            return _builtImage;
        }

        Log.info("Building image");

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
                                .cmd("java", "-ea", "-Xmx256M", "-XX:TieredStopAtLevel=1", "-XX:+UseParallelGC",
                                        "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
                                        "--add-exports", "java.base/jdk.internal.access=ALL-UNNAMED",
                                        "--add-opens=java.base/java.nio=ALL-UNNAMED",
                                        "-Ddhfs.objects.peerdiscovery.interval=1s",
                                        "-Ddhfs.objects.invalidation.delay=100",
                                        "-Ddhfs.objects.deletion.delay=0",
                                        "-Ddhfs.objects.deletion.can-delete-retry-delay=1000",
                                        "-Ddhfs.objects.ref_verification=true",
                                        "-Ddhfs.objects.sync.timeout=30",
                                        "-Ddhfs.objects.sync.ping.timeout=5",
                                        "-Ddhfs.objects.reconnect_interval=1s",
                                        "-Dquarkus.log.category.\"com.usatiuk\".level=TRACE",
                                        "-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=TRACE",
                                        "-Dquarkus.log.category.\"com.usatiuk.objects.transaction\".level=INFO",
                                        "-Ddhfs.objects.periodic-push-op-interval=5s",
                                        "-Ddhfs.fuse.root=/dhfs_test/fuse",
                                        "-Ddhfs.objects.persistence.files.root=/dhfs_test/data",
                                        "-Ddhfs.objects.persistence.stuff.root=/dhfs_test/data/stuff",
                                        "-jar", "/app/quarkus-run.jar")
                                .run("mkdir /dhfs_test && chmod 777 /dhfs_test")
                                .build())
                .withFileFromPath("/app", Paths.get(buildPath, "quarkus-app"))
                .withFileFromPath("/libs", Paths.get(nativeLibsDirectory));

        _builtImage = image.get();
        Log.info("Image built: " + _builtImage);
        return _builtImage;
    }
}
