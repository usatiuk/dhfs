package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.storage.SerializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.peersync.PeerSyncClient;
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
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class PersistentRemoteHostsService {
    @ConfigProperty(name = "dhfs.objects.distributed.root")
    String dataRoot;

    @Inject
    PeerSyncClient peerSyncClient;

    final String dataFileName = "hosts";

    private PersistentRemoteHosts _persistentData = new PersistentRemoteHosts();

    private UUID _selfUuid;

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading hosts");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }
        _selfUuid = _persistentData.runReadLocked(PersistentRemoteHostsData::getSelfUuid);
        Files.writeString(Paths.get(dataRoot, "self_uuid"), _selfUuid.toString());
        Log.info("Self uuid is: " + _selfUuid.toString());
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving hosts");
        Files.write(Paths.get(dataRoot).resolve(dataFileName), SerializationUtils.serialize(_persistentData));
        Log.info("Shutdown");
    }

    public UUID getSelfUuid() {
        if (_selfUuid == null)
            throw new IllegalStateException();
        else return _selfUuid;
    }

    public HostInfo getInfo(UUID name) {
        return _persistentData.runReadLocked(data -> {
            return data.getRemoteHosts().get(name);
        });
    }

    public List<HostInfo> getHosts() {
        return _persistentData.runReadLocked(data -> {
            return data.getRemoteHosts().values().stream().toList();
        });
    }

    public void addHost(HostInfo hostInfo) {
        if (hostInfo.getUuid().equals(_selfUuid)) return;
        boolean added = _persistentData.runWriteLocked(d -> {
            return d.getRemoteHosts().put(hostInfo.getUuid(), hostInfo) == null;
        });
        if (added) {
            // FIXME: async
            peerSyncClient.syncPeersAll();
        }
    }

    public boolean existsHost(UUID uuid) {
        return _persistentData.runReadLocked(d -> {
            return d.getRemoteHosts().containsKey(uuid);
        });
    }
}
