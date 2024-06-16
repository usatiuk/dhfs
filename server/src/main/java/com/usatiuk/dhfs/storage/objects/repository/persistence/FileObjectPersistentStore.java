package com.usatiuk.dhfs.storage.objects.repository.persistence;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    @ConfigProperty(name = "dhfs.objects.persistence.files.root")
    String root;

    @Inject
    Vertx vertx;

    void init(@Observes @Priority(200) StartupEvent event) {
        Paths.get(root).toFile().mkdirs();
        Log.info("Initializing with root " + root);
    }

    void shutdown(@Observes @Priority(400) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    @Nonnull
    @Override
    public List<String> findObjects(String prefix) {
        Path nsRoot = Paths.get(root);

        if (!nsRoot.toFile().isDirectory())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().readDir(nsRoot.toString()).onItem()
                .transformToMulti(v -> Multi.createFrom().iterable(v))
                .select().where(n -> n.startsWith(prefix))
                .map(f -> nsRoot.relativize(Paths.get(f)).toString()).collect().asList().await().indefinitely();
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        Path obj = Paths.get(root, name);

        return obj.toFile().isFile();
    }

    @Nonnull
    @Override
    public byte[] readObject(String name) {
        var file = Path.of(root, name);

        if (!file.toFile().exists())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().readFile(file.toString()).map(Buffer::getBytes).await().indefinitely();
    }

    @Nonnull
    @Override
    public Void writeObject(String name, byte[] data) {
        var file = Path.of(root, name);

        if (!Paths.get(root).toFile().isDirectory()
                && !Paths.get(root).toFile().mkdirs())
            throw new StatusRuntimeException(Status.INTERNAL);

        return vertx.fileSystem().writeFile(file.toString(), Buffer.buffer(data)).await().indefinitely();
    }

    @Nonnull
    @Override
    public Void deleteObject(String name) {
        var file = Path.of(root, name);

        if (!file.toFile().exists())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().delete(file.toString()).await().indefinitely();
    }
}
