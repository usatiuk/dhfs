package com.usatiuk.dhfs.storage.repository;

import com.usatiuk.dhfs.storage.data.Object;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class SimpleFileObjectRepository implements ObjectRepository {
    @ConfigProperty(name = "dhfs.filerepo.root")
    String root;

    @Inject
    Vertx vertx;

    @Startup
    void init() {
        if (!Paths.get(root).toFile().exists()) {
            Paths.get(root).toFile().mkdirs();
            Log.info("Creted root " + root);
        }

        Log.info("Initializing with root " + root);
    }

    @Shutdown
    void shutdown() {
        Log.info("Shutdown");
    }

    private Multi<String> TraverseDir(String dir) {
        return vertx.fileSystem().readDir(dir).onItem().transformToMulti(
                t -> {
                    List<Multi<String>> results = new ArrayList<>();
                    t.forEach(entry -> {
                        if (Paths.get(entry).toFile().isDirectory()) {
                            results.add(TraverseDir(Paths.get(entry).toString()));
                        } else {
                            results.add(Multi.createFrom().item(entry));
                        }
                    });
                    return Multi.createBy().merging().streams(results);
                }
        );

    }

    @Nonnull
    @Override
    public Multi<String> findObjects(String namespace, String prefix) {
        Path path = Paths.get(root, namespace, prefix);
        Path rootDir = path.toFile().isDirectory() ? path : path.getParent();
        String prefixInRoot = path.toFile().isDirectory() ? "" : path.getFileName().toString();
        return vertx.fileSystem().readDir(rootDir.toString()).onItem().transformToMulti(
                t -> {
                    List<Multi<String>> results = new ArrayList<>();
                    t.forEach(entry -> {
                        if (entry.startsWith(prefixInRoot)) {
                            if (Paths.get(entry).toFile().isDirectory()) {
                                results.add(TraverseDir(Paths.get(entry).toString()));
                            } else {
                                results.add(Multi.createFrom().item(entry));
                            }
                        }
                    });
                    return Multi.createBy().merging().streams(results);
                }
        ).map(f -> rootDir.relativize(Paths.get(f)).toString());
    }

    @Nonnull
    @Override
    public Uni<Object> readObject(String namespace, String name) {
        return null;
    }

    @Nonnull
    @Override
    public Uni<Void> writeObject(String namespace, String name, Object data) {
        return null;
    }
}
