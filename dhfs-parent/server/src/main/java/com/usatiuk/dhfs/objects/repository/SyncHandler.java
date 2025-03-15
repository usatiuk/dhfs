package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import com.usatiuk.dhfs.objects.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pcollections.PMap;

import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class SyncHandler {
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

    private final Map<Class<? extends JDataRemote>, ObjSyncHandler> _objToSyncHandler;
    private final Map<Class<? extends JDataRemoteDto>, ObjSyncHandler> _dtoToSyncHandler;

    public SyncHandler(Instance<ObjSyncHandler<?, ?>> syncHandlers) {
        HashMap<Class<? extends JDataRemote>, ObjSyncHandler> objToHandlerMap = new HashMap<>();
        HashMap<Class<? extends JDataRemoteDto>, ObjSyncHandler> dtoToHandlerMap = new HashMap<>();

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
    }

    public <D extends JDataRemoteDto> void handleRemoteUpdate(PeerId from, JObjectKey key,
                                                              PMap<PeerId, Long> receivedChangelog,
                                                              @Nullable D receivedData) {
        var got = Optional.ofNullable(receivedData).flatMap(d -> Optional.ofNullable(_dtoToSyncHandler.get(d.getClass()))).orElse(null);
        if (got == null) {
            assert receivedData == null || receivedData.objClass().equals(receivedData.getClass());
            defaultObjSyncHandler.handleRemoteUpdate(from, key, receivedChangelog, (JDataRemote) receivedData);
        } else {
            got.handleRemoteUpdate(from, key, receivedChangelog, receivedData);
        }
    }

    public void doInitialSync(PeerId peer) {
        txm.run(() -> {
            try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                while (it.hasNext()) {
                    var key = it.peekNextKey();
                    invalidationQueueService.pushInvalidationToOne(peer, key, true);
                    it.skip();
                }
            }
        });
    }
}