package com.usatiuk.dhfs.storage.objects.repository.persistence;

import com.usatiuk.dhfs.storage.objects.data.Namespace;
import com.usatiuk.dhfs.storage.objects.data.Object;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
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

    @Shutdown
    void shutdown() {
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

        return vertx.fileSystem().readFile(file.toString()).map(r -> new Object(new Namespace(namespace), name, r.getBytes()));
    }

    @Nonnull
    @Override
    public Uni<Void> writeObject(String namespace, Object object) {
        var file = Path.of(root, namespace, object.getName());

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

    @Nonnull
    @Override
    public Uni<Void> createNamespace(String namespace) {
        if (Paths.get(root, namespace).toFile().exists())
            return Uni.createFrom().voidItem();
        if (!Paths.get(root, namespace).toFile().mkdirs())
            throw new StatusRuntimeException(Status.INTERNAL);
        return Uni.createFrom().voidItem();
    }

    @Nonnull
    @Override
    public Uni<Void> deleteNamespace(String namespace) {
        if (!Paths.get(root, namespace).toFile().exists())
            throw new StatusRuntimeException(Status.NOT_FOUND);
        if (!Paths.get(root, namespace).toFile().delete())
            throw new StatusRuntimeException(Status.INTERNAL);
        return Uni.createFrom().voidItem();
    }
}
