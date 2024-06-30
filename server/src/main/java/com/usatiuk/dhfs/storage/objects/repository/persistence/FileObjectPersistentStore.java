package com.usatiuk.dhfs.storage.objects.repository.persistence;

import com.usatiuk.dhfs.storage.SerializationHelper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.SerializationException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    @ConfigProperty(name = "dhfs.objects.persistence.files.root")
    String root;

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

        var read = nsRoot.toFile().listFiles();
        if (read == null) return List.of();
        ArrayList<String> out = new ArrayList<>();
        for (var s : read) {
            var rel = nsRoot.relativize(s.toPath()).toString();
            if (rel.startsWith(prefix))
                out.add(rel);
        }
        return out;
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        Path obj = Paths.get(root, name);

        return obj.toFile().isFile();
    }

    @Nonnull
    @Override
    public Object readObject(String name) {
        var file = Path.of(root, name);

        try (FileInputStream in = new FileInputStream(file.toFile())) {
            return SerializationHelper.deserialize(in);
        } catch (FileNotFoundException f) {
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Not found: " + name));
        } catch (IOException e) {
            Log.error("Error reading file " + file, e);
            throw new StatusRuntimeException(Status.INTERNAL);
        }
    }

    @Override
    public void writeObject(String name, Object data) {
        var file = Path.of(root, name);

        try {
            try (var fc = new FileOutputStream(file.toFile(), false);
                 ObjectOutputStream out = new ObjectOutputStream(fc)) {
                out.writeObject(data);
            } catch (final IOException ex) {
                throw new SerializationException(ex);
            }
        } catch (Exception e) {
            Log.error("Error writing file " + file, e);
            throw new StatusRuntimeException(Status.INTERNAL);
        }
    }

    @Override
    public void deleteObject(String name) {
        var file = Path.of(root, name);

        try {
            Files.delete(file);
        } catch (NoSuchFileException ignored) {
        } catch (IOException e) {
            Log.error("Error deleting file " + file, e);
            throw new StatusRuntimeException(Status.INTERNAL);
        }
    }
}
