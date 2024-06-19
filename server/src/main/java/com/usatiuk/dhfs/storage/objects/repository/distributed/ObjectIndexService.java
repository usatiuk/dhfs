package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.storage.DeserializationHelper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

@ApplicationScoped
public class ObjectIndexService {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    ObjectIndex _index = new ObjectIndex();

    @ConfigProperty(name = "dhfs.objects.distributed.root")
    String metaRoot;
    final String metaFileName = "meta";

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(metaRoot).toFile().mkdirs();
        Log.info("Initializing with root " + metaRoot);
        if (Paths.get(metaRoot).resolve(metaFileName).toFile().exists()) {
            Log.info("Reading index");
            _index = DeserializationHelper.deserialize(Files.readAllBytes(Paths.get(metaRoot).resolve(metaFileName)));
        }
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving index");
        Files.write(Paths.get(metaRoot).resolve(metaFileName), SerializationUtils.serialize(_index));
        Log.info("Shutdown");
    }

    public boolean exists(String name) {
        return _index.exists(name);
    }

    public Optional<ObjectMeta> getMeta(String name) {
        return _index.get(name);
    }

    public ObjectMeta getOrCreateMeta(String name, String conflictResolver) {
        var ret = _index.getOrCreate(name, conflictResolver);
        ret.runWriteLocked(md -> {
            md.getChangelog().putIfAbsent(selfname, 0L);
            return null;
        });
        return ret;
    }

    @FunctionalInterface
    public interface ForAllFn {
        void apply(String name, ObjectMeta meta);
    }

    public void forAllRead(ForAllFn fn) {
        _index.runReadLocked((data) -> {
            for (var entry : data.getObjectMetaMap().entrySet()) {
                fn.apply(entry.getKey(), entry.getValue());
            }
            return null;
        });
    }
}
