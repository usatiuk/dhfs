package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.SyncHandler;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
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

    public long getId() {
        return curTx.getId();
    }

    private <T extends JDataRemote> RemoteObject<T> tryDownloadRemote(RemoteObject<T> obj) {
        MutableObject<RemoteObject<T>> success = new MutableObject<>(null);

        remoteObjectServiceClient.getObject(obj.key(), rcv -> {
            if (!obj.meta().knownType().isInstance(rcv.getRight().data()))
                throw new IllegalStateException("Object type mismatch: " + obj.meta().knownType() + " vs " + rcv.getRight().data().getClass());

            if (!rcv.getRight().changelog().equals(obj.meta().changelog())) {
                var updated = syncHandler.handleRemoteUpdate(rcv.getLeft(), obj.key(), obj, rcv.getRight().changelog());
                if (!rcv.getRight().changelog().equals(updated.meta().changelog()))
                    throw new IllegalStateException("Changelog mismatch, update failed?: " + rcv.getRight().changelog() + " vs " + updated.meta().changelog());
                success.setValue(updated.withData((T) rcv.getRight().data()));
            } else {
                success.setValue(obj.withData((T) rcv.getRight().data()));
            }
            return true;
        });

        curTx.put(success.getValue());
        return success.getValue();
    }

    @SuppressWarnings("unchecked")
    public <T extends JDataRemote> Optional<RemoteObject<T>> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return curTx.get(RemoteObject.class, key, strategy)
                .map(obj -> {
                    if (obj.data() != null && !type.isInstance(obj.data()))
                        throw new IllegalStateException("Object (real) type mismatch: " + obj.data().getClass() + " vs " + type);
                    if (!type.isAssignableFrom(obj.meta().knownType()))
                        throw new IllegalStateException("Object (meta) type mismatch: " + obj.meta().knownType() + " vs " + type);

                    if (obj.data() != null)
                        return obj;
                    else
                        return tryDownloadRemote(obj);
                });
    }

    public Optional<RemoteObjectMeta> getMeta(JObjectKey key, LockingStrategy strategy) {
        return curTx.get(RemoteObject.class, key, strategy).map(obj -> obj.meta());
    }

    public <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        return get(type, key, strategy).map(RemoteObject::data);
    }


    public <T extends JDataRemote> void put(RemoteObject<T> obj) {
        curTx.put(obj);
    }

    public <T extends JDataRemote> void put(T obj) {
        var cur = get((Class<T>) obj.getClass(), obj.key()).orElse(null);

        if (cur == null) {
            curTx.put(new RemoteObject<>(obj, persistentPeerDataService.getSelfUuid()));
            return;
        }

        if (cur.data() != null && cur.data().equals(obj))
            return;
        if (cur.data() != null && !cur.data().getClass().equals(obj.getClass()))
            throw new IllegalStateException("Object type mismatch: " + cur.data().getClass() + " vs " + obj.getClass());
        var newMeta = cur.meta();
        newMeta = newMeta.withChangelog(newMeta.changelog().plus(persistentPeerDataService.getSelfUuid(),
                newMeta.changelog().get(persistentPeerDataService.getSelfUuid()) + 1));
        var newObj = cur.withData(obj).withMeta(newMeta);
        curTx.put(newObj);
    }

    public <T extends JDataRemote> Optional<RemoteObject<T>> get(Class<T> type, JObjectKey key) {
        return get(type, key, LockingStrategy.OPTIMISTIC);
    }

    public Optional<RemoteObjectMeta> getMeta(JObjectKey key) {
        return getMeta(key, LockingStrategy.OPTIMISTIC);
    }

    public <T extends JDataRemote> Optional<T> getData(Class<T> type, JObjectKey key) {
        return getData(type, key, LockingStrategy.OPTIMISTIC);
    }
}
