package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetadata;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteObjectServiceClient;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    JObjectManager jObjectManager;

    @Override
    public ConflictResolutionResult resolve(String conflictHost,
                                            ObjectHeader conflictSource,
                                            String localName) {

        var oursData = jObjectManager.get(localName, Directory.class).orElseThrow(() -> new NotImplementedException("Oops"));
        var theirsData = remoteObjectServiceClient.getSpecificObject(conflictHost, conflictSource.getName());

        LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>();
        AtomicReference<ObjectMetadata> newMetadata = new AtomicReference<>();
        AtomicReference<Long> newMtime = new AtomicReference<>();
        AtomicReference<Long> newCtime = new AtomicReference<>();

        boolean wasChanged = oursData.runReadLocked((m, oursDir) -> {
            var oursHeader = oursData.runReadLocked(ObjectMetadata::toRpcHeader);
            var theirsHeader = theirsData.getLeft();

            var theirsDir = (Directory) DeserializationHelper.deserialize(theirsData.getRight());
            if (!theirsDir.getClass().equals(Directory.class)) {
                Log.error("Object type mismatch!");
                throw new NotImplementedException();
            }

            Directory first;
            Directory second;
            String otherHostname;
            if (oursDir.getMtime() >= theirsDir.getMtime()) {
                first = oursDir;
                second = theirsDir;
                otherHostname = conflictHost;
            } else {
                second = oursDir;
                first = theirsDir;
                otherHostname = selfname;
            }

            mergedChildren.putAll(first.getChildren());
            for (var entry : second.getChildren().entrySet()) {
                if (mergedChildren.containsKey(entry.getKey()) &&
                        !Objects.equals(mergedChildren.get(entry.getKey()), entry.getValue())) {
                    int i = 0;
                    do {
                        String name = entry.getKey() + ".conflict." + i + "." + otherHostname;
                        if (mergedChildren.containsKey(name)) {
                            i++;
                            continue;
                        }
                        mergedChildren.put(name, entry.getValue());
                        break;
                    } while (true);
                } else {
                    mergedChildren.put(entry.getKey(), entry.getValue());
                }
            }

            newMetadata.set(new ObjectMetadata(oursHeader.getName(), oursHeader.getConflictResolver(), m.getType()));

            for (var entry : oursHeader.getChangelog().getEntriesList()) {
                newMetadata.get().getChangelog().put(entry.getHost(), entry.getVersion());
            }
            for (var entry : theirsHeader.getChangelog().getEntriesList()) {
                newMetadata.get().getChangelog().merge(entry.getHost(), entry.getVersion(), Long::max);
            }

            boolean wasChangedR = mergedChildren.size() != first.getChildren().size();
            if (wasChangedR) {
                newMetadata.get().getChangelog().merge(selfname, 1L, Long::sum);
            }
            newMtime.set(first.getMtime());
            newCtime.set(first.getCtime());

            return wasChangedR;
        });

        oursData.runWriteLocked((m, d, bump) -> {
            if (wasChanged)
                if (m.getBestVersion() >= newMetadata.get().getOurVersion())
                    throw new NotImplementedException("Race when conflict resolving");

            if (m.getBestVersion() > newMetadata.get().getOurVersion())
                throw new NotImplementedException("Race when conflict resolving");

            d.setMtime(newMtime.get());
            d.setCtime(newCtime.get());
            d.getChildren().clear();
            d.getChildren().putAll(mergedChildren);

            m.getChangelog().clear();
            m.getChangelog().putAll(newMetadata.get().getChangelog());
            return null;
        });

        return ConflictResolutionResult.RESOLVED;
    }
}
