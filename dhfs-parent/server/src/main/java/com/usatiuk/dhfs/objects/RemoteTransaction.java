package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Optional;

@ApplicationScoped
public class RemoteTransaction {
    @Inject
    Transaction curTx;

    public long getId() {
        return curTx.getId();
    }

    public <T extends JDataRemote> Optional<T> get(Class<T> type, JObjectKey key, LockingStrategy strategy) {
        throw new NotImplementedException();
    }

    public <T extends JDataRemote> void put(JData obj) {
        throw new NotImplementedException();
    }

    public <T extends JDataRemote> Optional<T> get(Class<T> type, JObjectKey key) {
        return get(type, key, LockingStrategy.OPTIMISTIC);
    }
}
