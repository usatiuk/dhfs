package com.usatiuk.dhfs.storage.objects.repository.persistence;

import com.usatiuk.dhfs.storage.objects.data.Object;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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
    public Multi<String> findObjects(String namespace, String prefix) {
        Path nsRoot = Paths.get(root, namespace);

        if (!nsRoot.toFile().isDirectory())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().readDir(nsRoot.toString()).onItem()
                .transformToMulti(v -> Multi.createFrom().iterable(v))
                .select().where(n -> n.startsWith(prefix))
                .map(f -> nsRoot.relativize(Paths.get(f)).toString());
    }

    @Nonnull
    @Override
    public Uni<Boolean> existsObject(String namespace, String name) {
        Path obj = Paths.get(root, namespace, name);

        if (!obj.toFile().isFile())
            return Uni.createFrom().item(false);

        return Uni.createFrom().item(true);
    }

    @Nonnull
    @Override
    public Uni<Object> readObject(String namespace, String name) {
        var file = Path.of(root, namespace, name);

        if (!file.toFile().exists())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().readFile(file.toString()).map(r -> new Object(namespace, name, r.getBytes()));
    }

    @Nonnull
    @Override
    public Uni<Void> writeObject(String namespace, Object object) {
        var file = Path.of(root, namespace, object.getName());

        if (!Paths.get(root, namespace).toFile().isDirectory()
                && !Paths.get(root, namespace).toFile().mkdirs())
            throw new StatusRuntimeException(Status.INTERNAL);

        return vertx.fileSystem().writeFile(file.toString(), Buffer.buffer(object.getData()));
    }

    @Nonnull
    @Override
    public Uni<Void> deleteObject(String namespace, String name) {
        var file = Path.of(root, namespace, name);

        if (!file.toFile().exists())
            throw new StatusRuntimeException(Status.NOT_FOUND);

        return vertx.fileSystem().delete(file.toString());
    }
}
