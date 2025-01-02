package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.CachingObjectPersistentStore;
import com.usatiuk.dhfs.objects.transaction.TxRecord;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

@ApplicationScoped
public class WritebackObjectPersistentStore {
    @Inject
    CachingObjectPersistentStore delegate;
    @Inject
    TxWriteback txWriteback;

    @Nonnull
    Collection<JObjectKey> findAllObjects() {
        return delegate.findAllObjects();
    }

    @Nonnull
    Optional<JDataVersionedWrapper<?>> readObject(JObjectKey name) {
        var pending = txWriteback.getPendingWrite(name).orElse(null);
        return switch (pending) {
            case TxWriteback.PendingWrite write -> Optional.of(write.data());
            case TxWriteback.PendingDelete ignored -> Optional.empty();
            case null -> delegate.readObject(name);
            default -> throw new IllegalStateException("Unexpected value: " + pending);
        };
    }

    void commitTx(Collection<TxRecord.TxObjectRecord<?>> writes, long id) {
        var bundle = txWriteback.createBundle();
        try {
            for (var action : writes) {
                switch (action) {
                    case TxRecord.TxObjectRecordWrite<?> write -> {
                        Log.trace("Flushing object " + write.key());
                        bundle.commit(new JDataVersionedWrapper<>(write.data(), id));
                    }
                    case TxRecord.TxObjectRecordDeleted deleted -> {
                        Log.trace("Deleting object " + deleted.key());
                        bundle.delete(deleted.key());
                    }
                    default -> {
                        throw new TxCommitException("Unexpected value: " + action.key());
                    }
                }
            }
        } catch (Throwable t) {
            txWriteback.dropBundle(bundle);
            throw new TxCommitException(t.getMessage(), t);
        }

        Log.tracef("Committing transaction %d to storage", id);
        txWriteback.commitBundle(bundle);
    }
}
