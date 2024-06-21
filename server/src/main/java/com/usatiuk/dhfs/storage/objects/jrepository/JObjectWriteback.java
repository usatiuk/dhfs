package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;

import java.util.LinkedHashMap;

@ApplicationScoped
public class JObjectWriteback {

    @Inject
    ObjectPersistentStore objectPersistentStore;

    private final LinkedHashMap<String, JObject<?>> _objects = new LinkedHashMap<>();

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        flush();
    }

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @RunOnVirtualThread
    public void flush() {
        while (true) {
            JObject<?> obj;
            synchronized (this) {
                var entry = _objects.pollFirstEntry();
                if (entry == null) break;
                obj = entry.getValue();
            }
            obj.runReadLocked((m) -> {
                objectPersistentStore.writeObject("meta_" + m.getName(), SerializationUtils.serialize(m));
                if (obj.isResolved())
                    obj.runReadLocked((m2, d) -> {
                        objectPersistentStore.writeObject(m.getName(), SerializationUtils.serialize(d));
                        return null;
                    });
                return null;
            });
        }
    }

    public void remove(String name) {
        synchronized (this) {
            _objects.remove(name);
        }
    }

    public void markDirty(String name, JObject<?> object) {
        synchronized (this) {
            _objects.put(name, object);
        }
    }
}
