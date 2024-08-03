package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.OpQueueHelper;
import com.usatiuk.dhfs.objects.jkleppmanntree.helpers.StorageInterfaceService;
import com.usatiuk.kleppmanntree.AtomicClock;
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
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@ApplicationScoped
public class JKleppmannTreeManager {
    @Inject
    JPeerInterface jPeerInterface;
    @Inject
    StorageInterfaceService storageInterfaceService;
    @Inject
    OpQueueHelper opQueueHelper;

    @ConfigProperty(name = "dhfs.objects.root")
    String dataRoot;
    private static final String dataFileName = "trees";

    // FIXME: There should be something smarter...
    private ConcurrentHashMap<String, JKleppmannTreePersistentData> _persistentData = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JKleppmannTree> _trees = new ConcurrentHashMap<>();

    void init(@Observes @Priority(300) StartupEvent event) throws IOException {
        Paths.get(dataRoot).toFile().mkdirs();
        Log.info("Initializing with root " + dataRoot);
        if (Paths.get(dataRoot).resolve(dataFileName).toFile().exists()) {
            Log.info("Reading tree data");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        } else if (Paths.get(dataRoot).resolve(dataFileName + ".bak").toFile().exists()) {
            Log.warn("Reading tree data from backup");
            _persistentData = SerializationHelper.deserialize(Files.readAllBytes(Paths.get(dataRoot).resolve(dataFileName)));
        }

        // FIXME!: Handle unclean shutdowns
    }

    void shutdown(@Observes @Priority(300) ShutdownEvent event) throws IOException {
        Log.info("Saving tree data");
        writeData();
        Log.info("Saved tree data");
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

    public JKleppmannTree getTree(String name) {
        return _trees.computeIfAbsent(name, this::createTree);
    }

    private JKleppmannTree createTree(String name) {
        var pdata = _persistentData.computeIfAbsent(name, n -> new JKleppmannTreePersistentData(opQueueHelper, n, new AtomicClock()));
        pdata.restoreHelper(opQueueHelper);
        return new JKleppmannTree(pdata, storageInterfaceService, jPeerInterface);
    }
}
