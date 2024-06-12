package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.DirEntry;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    Vertx vertx;
    @Inject
    ObjectRepository objectRepository;

    final static String namespace = "dhfs_files";

    void init(@Observes @Priority(300) StartupEvent event) {
        Log.info("Initializing file service");
        if (!objectRepository.existsObject(namespace, new UUID(0, 0).toString()).await().indefinitely()) {
            objectRepository.createNamespace(namespace).await().indefinitely();
            objectRepository.writeObject(namespace, new UUID(0, 0).toString(),
                    ByteBuffer.wrap(SerializationUtils.serialize(
                            new Directory().setUuid(new UUID(0, 0)))
                    )).await().indefinitely();
        }
        getRoot().await().indefinitely();
    }

    @Shutdown
    void shutdown() {
        Log.info("Shutdown file service");
    }

    // Taken from SerializationUtils
    public static <T> T deserialize(final InputStream inputStream) {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), inputStream)) {
            final T obj = (T) in.readObject();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(final byte[] objectData) {
        return deserialize(new ByteArrayInputStream(objectData));
    }

    private Uni<DirEntry> readDirEntry(String uuid) {
        return objectRepository.readObject(namespace, uuid)
                .map(o -> deserialize(o.getData().array()));
    }

    private Uni<Optional<DirEntry>> traverse(Directory from, Path path) {
        if (path.getNameCount() == 0) return Uni.createFrom().item(Optional.of(from));
        for (var el : from.getChildren()) {
            if (el.getLeft().equals(path.getName(0).toString())) {
                var ref = readDirEntry(el.getRight().toString()).await().indefinitely();
                if (ref instanceof Directory) {
                    return traverse((Directory) ref, path.subpath(1, path.getNameCount()));
                } else {
                    return Uni.createFrom().item(Optional.empty());
                }
            }
        }
        return Uni.createFrom().item(Optional.empty());
    }

    @Override
    public Uni<Optional<DirEntry>> getDirEntry(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        return Uni.createFrom().item(found);
    }

    @Override
    public Uni<Optional<File>> open(String name) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(Optional.empty());
        if (!(found.get() instanceof File)) return Uni.createFrom().item(Optional.empty());
        return Uni.createFrom().item(Optional.of((File) found.get()));
    }

    @Override
    public Uni<Iterable<String>> readDir(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        if (found.isEmpty()) throw new IllegalArgumentException();
        if (!(found.get() instanceof Directory)) throw new IllegalArgumentException();

        var foundDir = (Directory) found.get();
        return Uni.createFrom().item(foundDir.getChildren().stream().map(Pair::getLeft).toList());
    }

    @Override
    public Uni<Directory> getRoot() {
        return readDirEntry(new UUID(0, 0).toString()).map(d -> (Directory) d);
    }
}
