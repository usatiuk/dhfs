package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdateReply;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    @Inject
    RemoteHostManager remoteHostManager;

    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    @Inject
    InvalidationQueueService invalidationQueueService;

    @Inject
    Instance<ConflictResolver> conflictResolvers;

    void init(@Observes @Priority(340) StartupEvent event) throws IOException {
        remoteHostManager.addConnectionSuccessHandler((host) -> {
            doInitialResync(host);
            return null;
        });
    }

    void shutdown(@Observes @Priority(240) ShutdownEvent event) throws IOException {
    }

    private void doInitialResync(String host) {
        var got = remoteObjectServiceClient.getIndex(host);
        for (var h : got.getObjectsList()) {
            handleRemoteUpdate(IndexUpdatePush.newBuilder()
                    .setSelfname(got.getSelfname()).setHeader(h).build());
        }
        // Push our index to the other peer too, as they might not request it if
        // they didn't thing we were disconnected
        List<String> toPush = new ArrayList<>();
        objectIndexService.forAllRead((name, meta) -> {
            toPush.add(name);
        });
        for (String name : toPush) {
            invalidationQueueService.pushInvalidationToOne(host, name);
        }
    }

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        var meta = objectIndexService.getOrCreateMeta(request.getHeader().getName(), request.getHeader().getConflictResolver());

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

            var conflict = data.getChangelog().get(selfname) > receivedSelfVer;

            if (conflict) {
                handleConflict(request.getSelfname(), request.getHeader(), data);
                return null;
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

    public void handleConflict(String conflictHost, ObjectHeader conflictSource,
                               ObjectMetaData localMeta) {
        var resolver = conflictResolvers.select(NamedLiteral.of(localMeta.getConflictResolver()));
        var theirs = remoteObjectServiceClient.getSpecificObject(conflictHost, conflictSource.getName());
        var oursData = objectPersistentStore.readObject(localMeta.getName());
        var res = resolver.get().resolve(oursData, localMeta.toRpcHeader(), theirs.getRight(), theirs.getLeft(), conflictHost);

        if (res.getType().equals(ConflictResolver.ConflictResolutionResult.Type.FAILED)) {
            Log.error("Failed resolving conflict");
            throw new NotImplementedException();
        }
        if (res.getType().equals(ConflictResolver.ConflictResolutionResult.Type.RESOLVED)) {
            Log.error("Resolved conflict for " + localMeta.getName());
            for (var obj : res.getResults()) {
                objectIndexService.getOrCreateMeta(obj.getLeft().getName(), obj.getLeft().getConflictResolver()).runWriteLocked(m -> {
                    m.getChangelog().clear();
                    for (var entry : obj.getLeft().getChangelog().getEntriesList()) {
                        m.getChangelog().put(entry.getHost(), entry.getVersion());
                    }
                    m.getChangelog().putIfAbsent(selfname, 0L);

                    objectPersistentStore.writeObject(m.getName(), obj.getRight());
                    return null;
                });
                invalidationQueueService.pushInvalidationToAll(obj.getLeft().getName());
            }
        }
    }
}
