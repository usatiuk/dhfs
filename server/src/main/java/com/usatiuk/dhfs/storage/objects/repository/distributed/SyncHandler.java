package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;

@ApplicationScoped
public class SyncHandler {
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

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    public void doInitialResync(UUID host) {
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

    public boolean tryHandleOneUpdate(UUID from, ObjectHeader header) {
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
                .getEntriesList().stream().filter(p -> p.getHost().equals(persistentRemoteHostsService.getSelfUuid().toString()))
                .findFirst().map(ObjectChangelogEntry::getVersion).orElse(0L);

        var receivedTotalVer = header.getChangelog().getEntriesList()
                .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

        boolean conflict = found.runWriteLockedMeta((md, bump, invalidate) -> {
            if (md.getRemoteCopies().getOrDefault(from, 0L) > receivedTotalVer) {
                Log.error("Received older index update than was known for host: "
                        + from + " " + header.getName());
                return false;
            }

            String rcv = "";
            for (var e : header.getChangelog().getEntriesList()) {
                rcv += e.getHost() + ": " + e.getVersion() + "; ";
            }
            String ours = "";
            for (var e : md.getChangelog().entrySet()) {
                ours += e.getKey() + ": " + e.getValue() + "; ";
            }
            Log.info("Handling update: " + header.getName() + " from " + from + "\n" + "ours: " + ours + " \n" + "received: " + rcv);


            md.getRemoteCopies().put(from, receivedTotalVer);

            var receivedMap = new HashMap<UUID, Long>();
            for (var e : header.getChangelog().getEntriesList()) {
                receivedMap.put(UUID.fromString(e.getHost()), e.getVersion());
            }

            for (var e : md.getChangelog().entrySet()) {
                if (receivedMap.getOrDefault(e.getKey(), 0L) < e.getValue()) {
                    Log.info("Conflict on update (lower version): " + header.getName() + " from " + from);
                    return true;
                }
            }

            if (Objects.equals(md.getOurVersion(), receivedTotalVer)) {
                for (var e : header.getChangelog().getEntriesList()) {
                    if (!Objects.equals(md.getChangelog().getOrDefault(UUID.fromString(e.getHost()), 0L),
                            e.getVersion())) {
                        Log.info("Conflict on update (other version): " + header.getName() + " from " + from);
                        return true;
                    }
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
                md.getChangelog().put(UUID.fromString(entry.getHost()), entry.getVersion());
            }
            md.getChangelog().putIfAbsent(persistentRemoteHostsService.getSelfUuid(), 0L);

            return false;
        });

        return !conflict;
    }

    private void handleOneUpdate(UUID from, ObjectHeader header) {
        if (!tryHandleOneUpdate(from, header)) {
            Log.info("Trying conflict resolution: " + header.getName() + " from " + from);
            JObject<?> found = jObjectManager.get(header.getName())
                    .orElseThrow(() -> new IllegalStateException("Object deleted when handling update?"));
            var resolver = conflictResolvers.select(found.getConflictResolver());
            var result = resolver.get().resolve(from, found);
            if (result.equals(ConflictResolver.ConflictResolutionResult.RESOLVED)) {
                Log.info("Resolved conflict for " + from + " " + header.getName());
            } else {
                Log.error("Failed conflict resolution for " + from + " " + header.getName());
                throw new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription("Conflict resolution failed"));
            }
        }
    }

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        var builder = IndexUpdateReply.newBuilder().setSelfUuid(persistentRemoteHostsService.getSelfUuid().toString());

        for (var u : request.getHeaderList()) {
            try {
                handleOneUpdate(UUID.fromString(request.getSelfUuid()), u);
            } catch (Exception ex) {
                Log.info("Error when handling update from " + request.getSelfUuid() + " of " + u.getName(), ex);
                builder.addErrors(IndexUpdateError.newBuilder().setObjectName(u.getName()).setError(ex.toString() + Arrays.toString(ex.getStackTrace())).build());
            }
        }
        return builder.build();
    }
}