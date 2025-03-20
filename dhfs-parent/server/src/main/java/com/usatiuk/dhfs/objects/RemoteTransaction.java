package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.SyncHandler;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Optional;

@ApplicationScoped
public class RemoteTransaction {
    @Inject
    Transaction curTx;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    SyncHandler syncHandler;
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    private <T extends JDataRemote> Optional<RemoteObjectDataWrapper<T>> tryDownloadRemote(RemoteObjectMeta obj) {
        MutableObject<RemoteObjectDataWrapper<T>> success = new MutableObject<>(null);

        try {
            remoteObjectServiceClient.getObject(obj.key(), rcv -> {
                if (!obj.knownType().isAssignableFrom(rcv.getRight().data().objClass()))
                    throw new IllegalStateException("Object type mismatch: " + obj.knownType() + " vs " + rcv.getRight().data().getClass());

                syncHandler.handleRemoteUpdate(rcv.getLeft(), obj.key(), rcv.getRight().changelog(), rcv.getRight().data());

                var now = curTx.get(RemoteObjectMeta.class, RemoteObjectMeta.ofMetaKey(obj.key())).orElse(null);
                assert now != null;

                if (!now.hasLocalData())
                    return false;

                var gotData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(obj.key())).orElse(null);
                assert gotData != null;

                success.setValue(gotData);
                return true;
            });
        } catch (Exception e) {
            Log.error("Failed to download object " + obj.key(), e);
            return Optional.empty();
        }

        return Optional.of(success.getValue());
    }

    @SuppressWarnings("unchecked")
    private <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key, LockingStrategy strategy, boolean tryRequest) {
        return curTx.get(RemoteObjectMeta.class, RemoteObjectMeta.ofMetaKey(key), strategy)
                .flatMap(obj -> {
                    if (obj.hasLocalData()) {
                        var realData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(key), strategy).orElse(null);
                        if (realData == null)
                            throw new IllegalStateException("Local data not found for " + key); // TODO: Race
                        if (!type.isInstance(realData.data()))
                            throw new IllegalStateException("Object type mismatch: " + realData.data().getClass() + " vs " + type);
                        return Optional.of((T) realData.data());
                    }
                    if (!tryRequest)
                        return Optional.empty();
                    return tryDownloadRemote(obj).map(wrapper -> (T) wrapper.data());
                });
    }

    public Optional<RemoteObjectMeta> getMeta(JObjectKey key, LockingStrategy strategy) {
        return curTx.get(RemoteObjectMeta.class, RemoteObjectMeta.ofMetaKey(key), strategy);
    }

    public <T extends JDataRemote> void putDataRaw(T obj) {
        var curMeta = getMeta(obj.key()).orElse(null);
        if (curMeta == null)
            throw new IllegalArgumentException("No data found for " + obj.key() + " when in putDataRaw");

        if (!curMeta.knownType().isAssignableFrom(obj.getClass()))
            throw new IllegalStateException("Object type mismatch: " + curMeta.knownType() + " vs " + obj.getClass());

        var newData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(obj.key()))
                .map(w -> w.withData(obj)).orElse(new RemoteObjectDataWrapper<>(obj));
        curTx.put(newData);
    }

    public <T extends JDataRemote> void putData(T obj) {
        var curMeta = getMeta(obj.key()).orElse(null);

        if (curMeta == null) {
            curTx.put(new RemoteObjectMeta(obj, persistentPeerDataService.getSelfUuid()));
            curTx.put(new RemoteObjectDataWrapper<>(obj));
            return;
        }

//        if (cur.data() != null && cur.data().equals(obj))
//            return;
        if (!curMeta.knownType().isAssignableFrom(obj.getClass()))
            throw new IllegalStateException("Object type mismatch: " + curMeta.knownType() + " vs " + obj.getClass());
        var newMeta = curMeta;
        newMeta = newMeta.withChangelog(newMeta.changelog().plus(persistentPeerDataService.getSelfUuid(),
                newMeta.changelog().get(persistentPeerDataService.getSelfUuid()) + 1));
        curTx.put(newMeta);
        var newData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(obj.key()))
                .map(w -> w.withData(obj)).orElse(new RemoteObjectDataWrapper<>(obj));
        curTx.put(newData);
    }

    public Optional<RemoteObjectMeta> getMeta(JObjectKey key) {
        return getMeta(key, LockingStrategy.OPTIMISTIC);
    }

    public <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key) {
        return getData(type, key, LockingStrategy.OPTIMISTIC, true);
    }

    public <T extends JDataRemote> Optional<T> getDataLocal(Class<T> type, JObjectKey key) {
        return getData(type, key, LockingStrategy.OPTIMISTIC, false);
    }

    public <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return getData(type, key, strategy, true);
    }

    public <T extends JDataRemote> Optional<T> getDataLocal(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return getData(type, key, strategy, false);
    }
}
