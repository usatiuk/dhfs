package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdateReply;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class SyncHandler {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    JObjectManager jObjectManager;

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        var meta = objectIndexService.getOrCreateMeta(request.getHeader().getName(), request.getHeader().getAssumeUnique());

        var receivedSelfVer = request.getHeader().getChangelog()
                .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

        var receivedTotalVer = request.getHeader().getChangelog().getEntriesList()
                .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

        meta.runWriteLocked((data) -> {
            if (data.getRemoteCopies().getOrDefault(request.getSelfname(), 0L) > receivedTotalVer) {
                Log.error("Received older index update than was known for host: "
                        + request.getSelfname() + " " + request.getHeader().getName());
                return null;
            }

            // Before or after conflict resolution?
            data.getRemoteCopies().put(request.getSelfname(), receivedTotalVer);

            var conflict = (data.getChangelog().get(selfname) > receivedSelfVer) && !data.getAssumeUnique();

            if (conflict) {
                Log.error("Conflict when updating: " + request.getHeader().getName());
                throw new NotImplementedException();
            }

            if (receivedTotalVer.equals(data.getTotalVersion())) {
                return null;
            }

            data.getChangelog().clear();
            for (var entry : request.getHeader().getChangelog().getEntriesList()) {
                data.getChangelog().put(entry.getHost(), entry.getVersion());
            }
            data.getChangelog().putIfAbsent(selfname, 0L);

            try {
                objectPersistentStore.deleteObject(request.getHeader().getName());
            } catch (StatusRuntimeException sx) {
                if (sx.getStatus() != Status.NOT_FOUND)
                    Log.info("Couldn't delete object from persistent store: ", sx);
            } catch (Exception e) {
                Log.info("Couldn't delete object from persistent store: ", e);
            }

            jObjectManager.invalidateJObject(data.getName());

            return null;
        });

        return IndexUpdateReply.newBuilder().build();
    }

}
