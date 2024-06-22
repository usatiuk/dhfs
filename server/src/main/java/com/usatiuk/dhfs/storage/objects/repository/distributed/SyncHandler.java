package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Objects;

@ApplicationScoped
public class SyncHandler {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

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

    public void doInitialResync(String host) {
        var got = remoteObjectServiceClient.getIndex(host);
        handleRemoteUpdate(got);
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

    private void handleOneUpdate(String from, ObjectHeader header) {
        JObject<?> found;
        try {
            found = jObjectManager.getOrPut(header.getName(), new ObjectMetadata(
                    header.getName(), header.getConflictResolver(),
                    (Class<? extends JObjectData>) Class.forName(header.getType(),
                            true, JObject.class.getClassLoader())
            ));
        } catch (ClassNotFoundException ex) {
            throw new NotImplementedException(ex);
        }

        var receivedSelfVer = header.getChangelog()
                .getEntriesList().stream().filter(p -> p.getHost().equals(selfname))
                .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

        var receivedTotalVer = header.getChangelog().getEntriesList()
                .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

        boolean conflict = found.runWriteLockedMeta((md, bump, invalidate) -> {
            if (md.getRemoteCopies().getOrDefault(from, 0L) > receivedTotalVer) {
                Log.error("Received older index update than was known for host: "
                        + from + " " + header.getName());
                return false;
            }

            if (md.getChangelog().getOrDefault(selfname, 0L) > receivedSelfVer) return true;

            md.getRemoteCopies().put(from, receivedTotalVer);

            if (Objects.equals(md.getOurVersion(), receivedTotalVer)) {
                for (var e : header.getChangelog().getEntriesList()) {
                    if (!Objects.equals(md.getChangelog().getOrDefault(e.getHost(), 0L),
                            e.getVersion())) return true;
                }
            }

            // TODO: recheck this
            if (md.getOurVersion() > receivedTotalVer) {
                Log.info("Received older index update than known: "
                        + from + " " + header.getName());
                return false;
            }

            // md.getBestVersion() > md.getTotalVersion() should also work
            if (receivedTotalVer > md.getOurVersion()) {
                invalidate.apply();
            }

            md.getChangelog().clear();
            for (var entry : header.getChangelog().getEntriesList()) {
                md.getChangelog().put(entry.getHost(), entry.getVersion());
            }
            md.getChangelog().putIfAbsent(selfname, 0L);

            return false;
        });

        if (conflict) {
            var resolver = conflictResolvers.select(found.getConflictResolver());
            var result = resolver.get().resolve(from, header, header.getName());
            if (result.equals(ConflictResolver.ConflictResolutionResult.RESOLVED)) {
                Log.info("Resolved conflict for " + from + " " + header.getName());
            } else {
                Log.error("Failed conflict resolution for " + from + " " + header.getName());
                throw new NotImplementedException();
            }
        }

    }

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        var builder = IndexUpdateReply.newBuilder().setSelfname(selfname);

        for (var u : request.getHeaderList()) {
            try {
                handleOneUpdate(request.getSelfname(), u);
            } catch (Exception ex) {
                builder.addErrors(IndexUpdateError.newBuilder().setObjectName(u.getName()).setError(ex.toString()).build());
            }
        }
        return builder.build();
    }
}