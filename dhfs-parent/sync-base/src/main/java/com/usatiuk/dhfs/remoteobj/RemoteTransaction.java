package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.rpc.RemoteObjectServiceClient;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.mutable.MutableObject;
import org.pcollections.HashTreePSet;

import java.util.Optional;

/**
 * Helper class for working with remote objects.
 */
@Singleton
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
    private <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key, boolean tryRequest) {
        return curTx.get(RemoteObjectMeta.class, RemoteObjectMeta.ofMetaKey(key))
                .flatMap(obj -> {
                    if (obj.hasLocalData()) {
                        var realData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(key)).orElse(null);
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

    /**
     * Get metadata for the object with the given key.
     *
     * @param key the key of the object
     * @return an Optional containing the metadata if it exists, or an empty Optional if it doesn't
     */
    public Optional<RemoteObjectMeta> getMeta(JObjectKey key) {
        return curTx.get(RemoteObjectMeta.class, RemoteObjectMeta.ofMetaKey(key));
    }

    /**
     * Put the data of a remote object into the storage, without incrementing the version vector.
     *
     * @param obj the object to put
     * @param <T> the type of the object
     */
    public <T extends JDataRemote> void putDataRaw(T obj) {
        var curMeta = getMeta(obj.key()).orElse(null);
        if (curMeta == null)
            throw new IllegalArgumentException("No data found for " + obj.key() + " when in putDataRaw");

        if (!curMeta.knownType().isAssignableFrom(obj.getClass()))
            throw new IllegalStateException("Object type mismatch: " + curMeta.knownType() + " vs " + obj.getClass());

        var newMeta = curMeta;
        newMeta = newMeta.withConfirmedDeletes(HashTreePSet.empty());
        curTx.put(newMeta);

        var newData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(obj.key()))
                .map(w -> w.withData(obj)).orElse(new RemoteObjectDataWrapper<>(obj));
        curTx.put(newData);
    }

    /**
     * Put the data of a remote object into the storage, creating a new object.
     * Should only be used when an object is known to be new. (for example, when it is created with a unique random key)
     *
     * @param obj the object to put
     * @param <T> the type of the object
     */
    public <T extends JDataRemote> void putDataNew(T obj) {
        curTx.putNew(new RemoteObjectMeta(obj, persistentPeerDataService.getSelfUuid()));
        curTx.putNew(new RemoteObjectDataWrapper<>(obj));
    }

    /**
     * Put the data of a remote object into the storage, either creating a new object or updating an existing one.
     * If the object already exists, its version vector is incremented.
     *
     * @param obj the object to put
     * @param <T> the type of the object
     */
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

        if (!curMeta.knownType().equals(obj.getClass()))
            newMeta = newMeta.withKnownType(obj.getClass());

        newMeta = newMeta.withConfirmedDeletes(HashTreePSet.empty());

        newMeta = newMeta.withChangelog(newMeta.changelog().plus(persistentPeerDataService.getSelfUuid(),
                newMeta.changelog().get(persistentPeerDataService.getSelfUuid()) + 1));
        curTx.put(newMeta);
        var newData = curTx.get(RemoteObjectDataWrapper.class, RemoteObjectMeta.ofDataKey(obj.key()))
                .map(w -> w.withData(obj)).orElse(new RemoteObjectDataWrapper<>(obj));
        curTx.put(newData);
    }

    /**
     * Get the data of a remote object with the given key.
     * The data can be downloaded from the remote object if it is not already present in the local storage.
     *
     * @param type the type of the object
     * @param key  the key of the object
     * @return an Optional containing the data if it exists, or an empty Optional if it doesn't
     */
    public <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key) {
        return getData(type, key, true);
    }

    /**
     * Get the data of a remote object with the given key.
     * The data will not be downloaded from the remote object if it is not already present in the local storage.
     *
     * @param type the type of the object
     * @param key  the key of the object
     * @return an Optional containing the data if it exists, or an empty Optional if it doesn't
     */
    public <T extends JDataRemote> Optional<T> getDataLocal(Class<T> type, JObjectKey key) {
        return getData(type, key, false);
    }

}
