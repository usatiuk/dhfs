package com.usatiuk.dhfs.objects.repository.persistence;

import com.google.protobuf.Message;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    private final String root;

    private final Path metaPath;
    private final Path dataPath;

    public FileObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        this.root = root;
        this.metaPath = Paths.get(root, "meta");
        this.dataPath = Paths.get(root, "data");
    }

    void init(@Observes @Priority(200) StartupEvent event) {
        if (!metaPath.toFile().exists()) {
            Log.info("Initializing with root " + root);
            metaPath.toFile().mkdirs();
            dataPath.toFile().mkdirs();
            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    metaPath.resolve(String.valueOf(i)).resolve(String.valueOf(j)).toFile().mkdirs();
                    dataPath.resolve(String.valueOf(i)).resolve(String.valueOf(j)).toFile().mkdirs();
                }
            }
        }
    }

    void shutdown(@Observes @Priority(400) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    private Pair<String, String> getDirPathComponents(@Nonnull String obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        int p2 = h & 0b00000000_00000000_00000000_11111111;
        return Pair.ofNonNull(String.valueOf(p1 >> 8), String.valueOf(p2));
    }

    private Path getMetaPath(@Nonnull String obj) {
        var components = getDirPathComponents(obj);
        return metaPath.resolve(components.getLeft()).resolve(components.getRight()).resolve(obj);
    }

    private Path getDataPath(@Nonnull String obj) {
        var components = getDirPathComponents(obj);
        return dataPath.resolve(components.getLeft()).resolve(components.getRight()).resolve(obj);
    }

    private void findAllObjectsImpl(Collection<String> out, Path path) {
        var read = path.toFile().listFiles();
        if (read == null) return;

        for (var s : read) {
            if (s.isDirectory()) {
                findAllObjectsImpl(out, s.toPath());
            } else {
                out.add(s.getName());
            }
        }
    }

    @Nonnull
    @Override
    public Collection<String> findAllObjects() {
        ArrayList<String> out = new ArrayList<>();
        findAllObjectsImpl(out, metaPath);
        return Collections.unmodifiableCollection(out);
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        return getMetaPath(name).toFile().isFile();
    }

    @Nonnull
    @Override
    public Boolean existsObjectData(String name) {
        return getDataPath(name).toFile().isFile();
    }

    private <T extends Message> T readObjectImpl(T defaultInstance, Path path) {
        try (var fsb = new FileInputStream(path.toFile());
             var fs = new BufferedInputStream(fsb, 1048576)) {
            return (T) defaultInstance.getParserForType().parseFrom(fs);
        } catch (FileNotFoundException | NoSuchFileException fx) {
            throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        } catch (IOException e) {
            Log.error("Error reading file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Nonnull
    @Override
    public JObjectDataP readObject(String name) {
        return readObjectImpl(JObjectDataP.getDefaultInstance(), getDataPath(name));
    }

    @Nonnull
    @Override
    public ObjectMetadataP readObjectMeta(String name) {
        return readObjectImpl(ObjectMetadataP.getDefaultInstance(), getMetaPath(name));
    }

    private void writeObjectImpl(Path path, Message data) {
        try {
            try (var fsb = new FileOutputStream(path.toFile(), false);
                 var fs = new BufferedOutputStream(fsb, 1048576)) {
                data.writeTo(fs);
            }
        } catch (IOException e) {
            Log.error("Error writing file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void writeObject(String name, JObjectDataP data) {
        writeObjectImpl(getDataPath(name), data);
    }

    @Override
    public void writeObjectMeta(String name, ObjectMetadataP data) {
        writeObjectImpl(getMetaPath(name), data);
    }

    private void deleteImpl(Path path) {
        try {
            Files.delete(path);
        } catch (NoSuchFileException ignored) {
        } catch (IOException e) {
            Log.error("Error deleting file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void deleteObjectData(String name) {
        deleteImpl(getDataPath(name));
    }

    @Override
    public void deleteObject(String name) {
        deleteImpl(getDataPath(name));
        // FIXME: Race?
        deleteImpl(getMetaPath(name));
    }
}
