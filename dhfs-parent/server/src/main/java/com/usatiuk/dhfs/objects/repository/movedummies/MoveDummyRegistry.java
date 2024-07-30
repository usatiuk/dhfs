package com.usatiuk.dhfs.objects.repository.movedummies;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.UUID;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@ApplicationScoped
public class MoveDummyRegistry {
    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;
    @ConfigProperty(name = "dhfs.objects.root")
    String dataRoot;
    private static final String dataFileName = "movedummies";

    // FIXME: DB when?
    private MoveDummyRegistryData _persistentData = new MoveDummyRegistryData();

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading move dummy data queue");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        } else if (Paths.get(dataRoot).resolve(dataFileName + ".bak").toFile().exists()) {
            Log.warn("Reading move dummy data from backup");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }
        // TODO: Handle unclean shutdowns...
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving move dummy data");
        writeData();
        Log.info("Saved move dummy data");
    }

    private void writeData() {
        try {
            if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists())
                Files.move(Paths.get(dataRoot).resolve(dataFileName), Paths.get(dataRoot).resolve(dataFileName + ".bak"), REPLACE_EXISTING);
            Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        } catch (IOException iex) {
            Log.error("Error writing deferred invalidations data", iex);
            throw new RuntimeException(iex);
        }
    }

    public void registerMovedRef(JObject<?> parent, String child) {
        parent.assertRWLock();
        if (Log.isTraceEnabled())
            Log.trace("Registered moved ref " + parent.getName() + "->" + child);
        for (var host : persistentRemoteHostsService.getHostsUuid()) {
            synchronized (this) {
                _persistentData.getMoveDummiesPending().put(host, new MoveDummyEntry(parent.getName(), child));
            }
        }
    }

    @FunctionalInterface
    public interface MoveDummyForHostFn<R> {
        R apply(Collection<MoveDummyEntry> hostsData);
    }

    public <T> T withPendingForHost(UUID host, MoveDummyForHostFn<T> fn) {
        synchronized (this) {
            return fn.apply(_persistentData.getMoveDummiesPending().get(host));
        }
    }

    public void commitForHost(UUID host, MoveDummyEntry entry) {
        if (Log.isTraceEnabled())
            Log.trace("Committing pushed move from " + host + " " + entry.parent() + "->" + entry.child());
        synchronized (this) {
            _persistentData.getMoveDummiesPending().get(host).remove(entry);
        }
    }

}
