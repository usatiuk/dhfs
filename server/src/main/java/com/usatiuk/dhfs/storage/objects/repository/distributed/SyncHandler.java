package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdateReply;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
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
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.Objects;

@ApplicationScoped
public class SyncHandler {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Inject
    ObjectPersistentStore objectPersistentStore;

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
        var objs = jObjectManager.find("");

        for (var obj : objs) {
            obj.runReadLocked((meta) -> {
                invalidationQueueService.pushInvalidationToOne(host, obj.getName());
                return null;
            });
        }
    }

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        JObject<?> found;
        try {
            found = jObjectManager.getOrPut(request.getHeader().getName(), new ObjectMetadata(
                    request.getHeader().getName(), request.getHeader().getConflictResolver(), (Class<? extends JObjectData>) Class.forName(request.getHeader().getType(), true, JObject.class.getClassLoader())
            ));
        } catch (ClassNotFoundException ex) {
            throw new NotImplementedException(ex);
        }

        var receivedSelfVer = request.getHeader().getChangelog()
                .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

        var receivedTotalVer = request.getHeader().getChangelog().getEntriesList()
                .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

        boolean conflict = found.runWriteLockedMeta((md, bump, invalidate) -> {
            if (md.getRemoteCopies().getOrDefault(request.getSelfname(), 0L) > receivedTotalVer) {
                Log.error("Received older index update than was known for host: "
                        + request.getSelfname() + " " + request.getHeader().getName());
                return false;
            }

            if (md.getChangelog().getOrDefault(selfname, 0L) > receivedSelfVer) return true;

            md.getRemoteCopies().put(request.getSelfname(), receivedTotalVer);

            if (Objects.equals(md.getOurVersion(), receivedTotalVer)) {
                for (var e : request.getHeader().getChangelog().getEntriesList()) {
                    if (!Objects.equals(md.getChangelog().getOrDefault(e.getHost(), 0L),
                            e.getVersion())) return true;
                }
            }

            // TODO: recheck this
            if (md.getOurVersion() > receivedTotalVer) {
                Log.info("Received older index update than known: "
                        + request.getSelfname() + " " + request.getHeader().getName());
                return false;
            }

            // md.getBestVersion() > md.getTotalVersion() should also work
            if (receivedTotalVer > md.getOurVersion()) {
                invalidate.apply();
                try {
                    Log.info("Deleting " + request.getHeader().getName() + " as per invalidation from " + request.getSelfname());
                    objectPersistentStore.deleteObject(request.getHeader().getName());
                } catch (StatusRuntimeException sx) {
                    if (sx.getStatus() != Status.NOT_FOUND)
                        Log.info("Couldn't delete object from persistent store: ", sx);
                } catch (Exception e) {
                    Log.info("Couldn't delete object from persistent store: ", e);
                }
            }

            md.getChangelog().clear();
            for (var entry : request.getHeader().getChangelog().getEntriesList()) {
                md.getChangelog().put(entry.getHost(), entry.getVersion());
            }
            md.getChangelog().putIfAbsent(selfname, 0L);

            return false;
        });

        if (conflict) {
            var resolver = conflictResolvers.select(found.getConflictResolver());
            var result = resolver.get().resolve(request.getSelfname(), request.getHeader(), request.getHeader().getName());
            if (result.equals(ConflictResolver.ConflictResolutionResult.RESOLVED)) {
                Log.info("Resolved conflict for " + request.getSelfname() + " " + request.getHeader().getName());
            } else {
                Log.error("Failed conflict resolution for " + request.getSelfname() + " " + request.getHeader().getName());
                throw new NotImplementedException();
            }
        }


        return IndexUpdateReply.newBuilder().build();
    }
}
