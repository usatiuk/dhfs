package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.ShutdownChecker;
import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.peersync.InitialSyncProcessor;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.rpc.RemoteObjectServiceClient;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;

import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Handles synchronization of remote objects.
 */
@ApplicationScoped
public class SyncHandler {
    private final Map<Class<? extends JDataRemote>, ObjSyncHandler> _objToSyncHandler;
    private final Map<Class<? extends JDataRemoteDto>, ObjSyncHandler> _dtoToSyncHandler;
    private final Map<Class<? extends JData>, InitialSyncProcessor> _initialSyncProcessors;
    @Inject
    Transaction curTx;
    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    TransactionManager txm;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    DefaultObjSyncHandler defaultObjSyncHandler;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    ShutdownChecker shutdownChecker;

    public SyncHandler(Instance<ObjSyncHandler<?, ?>> syncHandlers, Instance<InitialSyncProcessor<?>> initialSyncProcessors) {
        HashMap<Class<? extends JDataRemote>, ObjSyncHandler> objToHandlerMap = new HashMap<>();
        HashMap<Class<? extends JDataRemoteDto>, ObjSyncHandler> dtoToHandlerMap = new HashMap<>();
        HashMap<Class<? extends JData>, InitialSyncProcessor> initialSyncProcessorHashMap = new HashMap<>();

        for (var syncHandler : syncHandlers.handles()) {
            for (var type : Arrays.stream(syncHandler.getBean().getBeanClass().getGenericInterfaces()).flatMap(
                    t -> {
                        if (!(t instanceof ParameterizedType pm)) return Stream.empty();
                        if (pm.getRawType().equals(ObjSyncHandler.class)) return Stream.of(pm);
                        return Stream.empty();
                    }
            ).toList()) {
                var orig = type.getActualTypeArguments()[0];
                var dto = type.getActualTypeArguments()[1];
                assert JDataRemote.class.isAssignableFrom((Class<?>) orig);
                assert JDataRemoteDto.class.isAssignableFrom((Class<?>) dto);
                objToHandlerMap.put((Class<? extends JDataRemote>) orig, syncHandler.get());
                dtoToHandlerMap.put((Class<? extends JDataRemoteDto>) dto, syncHandler.get());
            }
        }

        _objToSyncHandler = Map.copyOf(objToHandlerMap);
        _dtoToSyncHandler = Map.copyOf(dtoToHandlerMap);

        for (var initialSyncProcessor : initialSyncProcessors.handles()) {
            for (var type : Arrays.stream(initialSyncProcessor.getBean().getBeanClass().getGenericInterfaces()).flatMap(
                    t -> {
                        if (!(t instanceof ParameterizedType pm)) return Stream.empty();
                        if (pm.getRawType().equals(InitialSyncProcessor.class)) return Stream.of(pm);
                        return Stream.empty();
                    }
            ).toList()) {
                var orig = type.getActualTypeArguments()[0];
                assert JData.class.isAssignableFrom((Class<?>) orig);
                initialSyncProcessorHashMap.put((Class<? extends JData>) orig, initialSyncProcessor.get());
            }
        }

        _initialSyncProcessors = Map.copyOf(initialSyncProcessorHashMap);
    }

    /**
     * Handles remote update of an object.
     *
     * @param from              the ID of the peer that sent the update
     * @param key               the key of the object
     * @param receivedChangelog the changelog received from the peer
     * @param receivedData      the data received from the peer
     * @param <D>               the type of the remote object DTO
     */
    public <D extends JDataRemoteDto> void handleRemoteUpdate(PeerId from, JObjectKey key,
                                                              PMap<PeerId, Long> receivedChangelog,
                                                              @Nullable D receivedData) {
        var current = remoteTx.getMeta(key).orElse(null);

        if (current != null) {
            current = current.withConfirmedDeletes(HashTreePSet.empty());
            curTx.put(current);
        }

        if (receivedData == null) {
            if (current != null) {
                var cmp = SyncHelper.compareChangelogs(current.changelog(), receivedChangelog);
                if (cmp.equals(SyncHelper.ChangelogCmpResult.CONFLICT)) {
                    var got = remoteObjectServiceClient.getSpecificObject(key, from);
                    handleRemoteUpdate(from, key, got.getRight().changelog(), got.getRight().data());
                    return;
                }
            }
        }

        var got = Optional.ofNullable(receivedData).flatMap(d -> Optional.ofNullable(_dtoToSyncHandler.get(d.getClass()))).orElse(null);
        if (got == null) {
            assert receivedData == null || receivedData.objClass().equals(receivedData.getClass());
            defaultObjSyncHandler.handleRemoteUpdate(from, key, receivedChangelog, (JDataRemote) receivedData);
        } else {
            got.handleRemoteUpdate(from, key, receivedChangelog, receivedData);
        }
    }

    /**
     * Resync objects after a crash.
     */
    public void resyncAfterCrash(@Observes @Priority(100000) StartupEvent event) {
        if (shutdownChecker.lastShutdownClean())
            return;
        txm.run(() -> {
            try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                while (it.hasNext()) {
                    var key = it.peekNextKey();
                    txm.run(() -> {
                        var proc = curTx.get(JData.class, key).flatMap(o -> Optional.ofNullable(_initialSyncProcessors.get(o.getClass()))).orElse(null);
                        if (proc != null) {
                            proc.handleCrash(key);
                        }
                        Log.infov("Handled crash of {0}", key);

                    }, true);
                    it.skip();
                }
            }
        });
    }

    /**
     * Do initial sync for a newly connected (or reconnected) peer.
     *
     * @param peer the ID of the peer
     */
    public void doInitialSync(PeerId peer) {
        txm.run(() -> {
            Log.tracev("Will do initial sync for {0}", peer);
            try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                while (it.hasNext()) {
                    var key = it.peekNextKey();
                    txm.run(() -> {
                        var proc = curTx.get(JData.class, key).flatMap(o -> Optional.ofNullable(_initialSyncProcessors.get(o.getClass()))).orElse(null);
                        if (proc != null) {
                            proc.prepareForInitialSync(peer, key);
                        }
                        Log.infov("Adding to initial sync for peer {0}: {1}", peer, key);
                        invalidationQueueService.pushInvalidationToOne(peer, key);

                    }, true);
                    it.skip();
                }
            }
        });
    }
}