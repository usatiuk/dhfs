package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.JObjectTxManager;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.repository.opsupport.OpObjectRegistry;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApplicationScoped
public class SyncHandler {
    @Inject
    JObjectManager jObjectManager;
    @Inject
    PeerManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    Instance<ConflictResolver> conflictResolvers;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    ProtoSerializerService protoSerializerService;
    @Inject
    OpObjectRegistry opObjectRegistry;
    @Inject
    JObjectTxManager jObjectTxManager;

    public void pushInitialResyncObj(UUID host) {
        Log.info("Doing initial object push for " + host);

        var objs = jObjectManager.findAll();

        for (var obj : objs) {
            Log.trace("IS: " + obj + " to " + host);
            invalidationQueueService.pushInvalidationToOne(host, obj);
        }
    }

    public void pushInitialResyncOp(UUID host) {
        Log.info("Doing initial op push for " + host);

        opObjectRegistry.pushBootstrapData(host);
    }

    public void handleOneUpdate(UUID from, ObjectHeader header) {
        jObjectTxManager.executeTx(() -> {
            JObject<?> found = jObjectManager.getOrPut(header.getName(), JObjectData.class, Optional.empty());

            var receivedTotalVer = header.getChangelog().getEntriesList()
                    .stream().map(ObjectChangelogEntry::getVersion).reduce(0L, Long::sum);

            var receivedMap = new HashMap<UUID, Long>();
            for (var e : header.getChangelog().getEntriesList()) {
                receivedMap.put(UUID.fromString(e.getHost()), e.getVersion());
            }

            boolean conflict = found.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION, (md, data, bump, invalidate) -> {
                if (md.getRemoteCopies().getOrDefault(from, 0L) > receivedTotalVer) {
                    Log.error("Received older index update than was known for host: "
                            + from + " " + header.getName());
                    throw new OutdatedUpdateException();
                }

                String rcv = "";
                for (var e : header.getChangelog().getEntriesList()) {
                    rcv += e.getHost() + ": " + e.getVersion() + "; ";
                }
                String ours = "";
                for (var e : md.getChangelog().entrySet()) {
                    ours += e.getKey() + ": " + e.getValue() + "; ";
                }
                Log.trace("Handling update: " + header.getName() + " from " + from + "\n" + "ours: " + ours + " \n" + "received: " + rcv);

                boolean updatedRemoteVersion = false;

                var oldRemoteVer = md.getRemoteCopies().put(from, receivedTotalVer);
                if (oldRemoteVer == null || !oldRemoteVer.equals(receivedTotalVer)) updatedRemoteVersion = true;

                boolean hasLower = false;
                boolean hasHigher = false;
                for (var e : Stream.concat(md.getChangelog().keySet().stream(), receivedMap.keySet().stream()).collect(Collectors.toSet())) {
                    if (receivedMap.getOrDefault(e, 0L) < md.getChangelog().getOrDefault(e, 0L))
                        hasLower = true;
                    if (receivedMap.getOrDefault(e, 0L) > md.getChangelog().getOrDefault(e, 0L))
                        hasHigher = true;
                }

                if (hasLower && hasHigher) {
                    Log.info("Conflict on update (inconsistent version): " + header.getName() + " from " + from);
                    return true;
                }

                if (hasLower) {
                    Log.info("Received older index update than known: "
                            + from + " " + header.getName());
                    throw new OutdatedUpdateException();
                }

                if (hasHigher) {
                    invalidate.apply();
                    md.getChangelog().clear();
                    md.getChangelog().putAll(receivedMap);
                    md.getChangelog().putIfAbsent(persistentPeerDataService.getSelfUuid(), 0L);
                    if (header.hasPushedData())
                        found.externalResolution(protoSerializerService.deserialize(header.getPushedData()));
                    return false;
                } else if (data == null && header.hasPushedData()) {
                    found.tryResolve(JObjectManager.ResolutionStrategy.LOCAL_ONLY);
                    if (found.getData() == null)
                        found.externalResolution(protoSerializerService.deserialize(header.getPushedData()));
                }

                assert Objects.equals(receivedTotalVer, md.getOurVersion());

                if (!updatedRemoteVersion)
                    Log.debug("No action on update: " + header.getName() + " from " + from);

                return false;
            });

            if (conflict) {
                Log.info("Trying conflict resolution: " + header.getName() + " from " + from);

                JObjectData theirsData;
                ObjectHeader theirsHeader;
                if (header.hasPushedData()) {
                    theirsHeader = header;
                    theirsData = protoSerializerService.deserialize(header.getPushedData());
                } else {
                    var got = remoteObjectServiceClient.getSpecificObject(from, header.getName());
                    theirsData = protoSerializerService.deserialize(got.getRight());
                    theirsHeader = got.getLeft();
                }

                var resolverClass = found.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                    if (d == null)
                        throw new StatusRuntimeExceptionNoStacktrace(Status.UNAVAILABLE.withDescription("No local data when conflict " + header.getName()));
                    return d.getConflictResolver();
                });
                var resolver = conflictResolvers.select(resolverClass);
                resolver.get().resolve(from, theirsHeader, theirsData, found);
                Log.info("Resolved conflict for " + from + " " + header.getName());
            }
        });
    }

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        // TODO: Dedup
        try {
            handleOneUpdate(UUID.fromString(request.getSelfUuid()), request.getHeader());
        } catch (OutdatedUpdateException ignored) {
            Log.warn("Outdated update of " + request.getHeader().getName() + " from " + request.getSelfUuid());
            invalidationQueueService.pushInvalidationToOne(UUID.fromString(request.getSelfUuid()), request.getHeader().getName());
        } catch (Exception ex) {
            Log.info("Error when handling update from " + request.getSelfUuid() + " of " + request.getHeader().getName(), ex);
            throw ex;
        }

        return IndexUpdateReply.getDefaultInstance();
    }

    protected static class OutdatedUpdateException extends RuntimeException {
        OutdatedUpdateException() {
            super();
        }

        OutdatedUpdateException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}